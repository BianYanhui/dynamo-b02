use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};
use smallvec::SmallVec;

const KIND_UNKNOWN: u8 = 0;
const KIND_STORED: u8 = 1;
const KIND_REMOVED: u8 = 2;
const KIND_CLEARED: u8 = 3;

#[derive(Default)]
struct SelectorStats {
    input_batches: u64,
    input_events: u64,
    output_batches: u64,
    output_events: u64,
    merged_events: u64,
    duplicate_events: u64,
    invalidation_events: u64,
}

#[pyclass(module = "kv_event_selector")]
struct RustKVEventSelector {
    max_pending_events: usize,
    pending: Vec<Py<PyAny>>,
    pending_last_kind: u8,
    kind_by_type: HashMap<usize, u8>,
    known_hashes: HashSet<u64>,
    removed_hashes: HashSet<u64>,
    stats: SelectorStats,
}

#[pymethods]
impl RustKVEventSelector {
    #[new]
    fn new(max_pending_events: usize) -> PyResult<Self> {
        if max_pending_events == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "max_pending_events must be positive",
            ));
        }
        Ok(Self {
            max_pending_events,
            pending: Vec::with_capacity(max_pending_events.min(4096)),
            pending_last_kind: KIND_UNKNOWN,
            kind_by_type: HashMap::with_capacity(4),
            known_hashes: HashSet::new(),
            removed_hashes: HashSet::new(),
            stats: SelectorStats::default(),
        })
    }

    fn ingest(&mut self, events: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = events.py();
        for item in events.try_iter()? {
            let event = item?;
            self.stats.input_events += 1;
            match self.event_kind(&event)? {
                KIND_STORED => self.ingest_stored(py, &event)?,
                KIND_REMOVED => self.ingest_removed(py, &event)?,
                KIND_CLEARED => {
                    self.pending.clear();
                    self.known_hashes.clear();
                    self.removed_hashes.clear();
                    self.pending.push(event.unbind());
                    self.pending_last_kind = KIND_CLEARED;
                    self.stats.invalidation_events += 1;
                }
                _ => {
                    self.pending.push(event.unbind());
                    self.pending_last_kind = KIND_UNKNOWN;
                }
            }
        }
        Ok(())
    }

    fn flush<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let pending = std::mem::take(&mut self.pending);
        let output = PyList::empty(py);
        for event in pending {
            output.append(event.bind(py))?;
        }
        self.pending_last_kind = KIND_UNKNOWN;
        self.stats.output_events += output.len() as u64;
        if output.len() > 0 {
            self.stats.output_batches += 1;
        }
        Ok(output)
    }

    fn should_flush(&self) -> bool {
        self.pending.len() >= self.max_pending_events
    }

    fn pending_count(&self) -> usize {
        self.pending.len()
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let result = PyDict::new(py);
        result.set_item("input_batches", self.stats.input_batches)?;
        result.set_item("input_events", self.stats.input_events)?;
        result.set_item("output_batches", self.stats.output_batches)?;
        result.set_item("output_events", self.stats.output_events)?;
        result.set_item("merged_events", self.stats.merged_events)?;
        result.set_item("duplicate_events", self.stats.duplicate_events)?;
        result.set_item("invalidation_events", self.stats.invalidation_events)?;
        Ok(result)
    }

    fn record_input_batch(&mut self) {
        self.stats.input_batches += 1;
    }

    fn reset(&mut self) {
        self.pending.clear();
        self.pending_last_kind = KIND_UNKNOWN;
        self.known_hashes.clear();
        self.removed_hashes.clear();
    }

    fn event_kind(&mut self, event: &Bound<'_, PyAny>) -> PyResult<u8> {
        let event_type = event.get_type();
        let type_key = event_type.as_ptr() as usize;
        if let Some(kind) = self.kind_by_type.get(&type_key) {
            return Ok(*kind);
        }
        let type_name = event_type.name()?;
        let name = type_name.to_str()?;
        let kind = match name {
            "BlockStored" => KIND_STORED,
            "BlockRemoved" => KIND_REMOVED,
            "AllBlocksCleared" => KIND_CLEARED,
            _ => KIND_UNKNOWN,
        };
        self.kind_by_type.insert(type_key, kind);
        Ok(kind)
    }
}

impl RustKVEventSelector {
    fn ingest_stored(&mut self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let hashes = block_hashes(event)?;
        if !hashes.is_empty()
            && ((hashes.len() == 1 && self.known_hashes.contains(&hashes[0]))
                || (hashes.len() > 1
                    && hashes
                        .iter()
                        .all(|block_hash| self.known_hashes.contains(block_hash))))
        {
            self.stats.duplicate_events += 1;
            return Ok(());
        }

        for block_hash in &hashes {
            self.removed_hashes.remove(block_hash);
        }

        if self.pending_last_kind == KIND_STORED && hashes.len() == 1 {
            let parent = optional_u64_attr(event, "parent_block_hash")?;
            if let Some(previous) = self.pending.last() {
                let previous_bound = previous.bind(py);
                let previous_hashes = block_hashes(previous_bound)?;
                if let Some(parent_hash) = parent {
                    if can_merge(py, previous_bound, event, &previous_hashes, parent_hash)? {
                        let merged =
                            merge_stored(py, previous_bound, event, &previous_hashes, &hashes)?;
                        *self.pending.last_mut().expect("pending checked above") = merged;
                        self.stats.merged_events += 1;
                        self.known_hashes.insert(hashes[0]);
                        return Ok(());
                    }
                }
            }
        }

        self.pending.push(event.clone().unbind());
        for block_hash in hashes {
            self.known_hashes.insert(block_hash);
        }
        self.pending_last_kind = KIND_STORED;
        Ok(())
    }

    fn ingest_removed(&mut self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let hashes = block_hashes(event)?;
        if hashes.len() == 1 {
            let block_hash = hashes[0];
            if self.removed_hashes.contains(&block_hash) {
                self.stats.duplicate_events += 1;
                return Ok(());
            }
            self.removed_hashes.insert(block_hash);
            self.known_hashes.remove(&block_hash);
            self.pending.push(event.clone().unbind());
            self.pending_last_kind = KIND_REMOVED;
            self.stats.invalidation_events += 1;
            return Ok(());
        }

        if !hashes.is_empty() {
            let fresh: Vec<u64> = hashes
                .iter()
                .copied()
                .filter(|block_hash| !self.removed_hashes.contains(block_hash))
                .collect();
            if fresh.is_empty() {
                self.stats.duplicate_events += 1;
                return Ok(());
            }
            if fresh.len() != hashes.len() {
                let filtered = copy_with_hashes(py, event, &fresh)?;
                self.pending.push(filtered);
            } else {
                self.pending.push(event.clone().unbind());
            }
            for block_hash in fresh {
                self.removed_hashes.insert(block_hash);
                self.known_hashes.remove(&block_hash);
            }
        } else {
            self.pending.push(event.clone().unbind());
        }
        self.pending_last_kind = KIND_REMOVED;
        self.stats.invalidation_events += 1;
        Ok(())
    }
}

fn block_hashes(event: &Bound<'_, PyAny>) -> PyResult<SmallVec<[u64; 1]>> {
    match event.getattr("block_hashes") {
        Ok(value) if !value.is_none() => {
            let length = value.len()?;
            let mut hashes = SmallVec::new();
            if length == 1 {
                hashes.push(value.get_item(0)?.extract::<u64>()?);
            } else {
                for item in value.try_iter()? {
                    hashes.push(item?.extract::<u64>()?);
                }
            }
            Ok(hashes)
        }
        Ok(_) => Ok(SmallVec::new()),
        Err(error) if error.is_instance_of::<pyo3::exceptions::PyAttributeError>(event.py()) => {
            Ok(SmallVec::new())
        }
        Err(error) => Err(error),
    }
}

fn optional_u64_attr(event: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<u64>> {
    match event.getattr(name) {
        Ok(value) if value.is_none() => Ok(None),
        Ok(value) => value.extract::<u64>().map(Some),
        Err(error) if error.is_instance_of::<pyo3::exceptions::PyAttributeError>(event.py()) => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn py_equal(left: &Bound<'_, PyAny>, right: &Bound<'_, PyAny>) -> PyResult<bool> {
    left.eq(right)
}

fn can_merge(
    py: Python<'_>,
    previous: &Bound<'_, PyAny>,
    event: &Bound<'_, PyAny>,
    previous_hashes: &[u64],
    parent: u64,
) -> PyResult<bool> {
    if previous_hashes.is_empty() || previous_hashes.last().copied() != Some(parent) {
        return Ok(false);
    }
    let previous_block_size = previous.getattr("block_size");
    let event_block_size = event.getattr("block_size");
    let previous_lora = previous.getattr("lora_name");
    let event_lora = event.getattr("lora_name");
    match (
        previous_block_size,
        event_block_size,
        previous_lora,
        event_lora,
    ) {
        (Ok(previous_block_size), Ok(event_block_size), Ok(previous_lora), Ok(event_lora)) => {
            Ok(py_equal(&previous_block_size, &event_block_size)?
                && py_equal(&previous_lora, &event_lora)?)
        }
        (Err(error), _, _, _)
        | (_, Err(error), _, _)
        | (_, _, Err(error), _)
        | (_, _, _, Err(error))
            if !error.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) =>
        {
            Err(error)
        }
        _ => Ok(false),
    }
}

fn append_sequence_attr(
    py: Python<'_>,
    target: &Bound<'_, PyList>,
    object: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<bool> {
    match object.getattr(name) {
        Ok(value) if !value.is_none() => {
            for item in value.try_iter()? {
                target.append(item?)?;
            }
            let _ = py;
            Ok(true)
        }
        Ok(_) => Ok(false),
        Err(error) if error.is_instance_of::<pyo3::exceptions::PyAttributeError>(object.py()) => {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn copy_with_hashes(
    py: Python<'_>,
    event: &Bound<'_, PyAny>,
    hashes: &[u64],
) -> PyResult<Py<PyAny>> {
    let copy_module = py.import("copy")?;
    let copied = copy_module.call_method1("copy", (event,))?;
    copied.setattr("block_hashes", PyList::new(py, hashes)?)?;
    Ok(copied.unbind())
}

fn merge_stored(
    py: Python<'_>,
    previous: &Bound<'_, PyAny>,
    event: &Bound<'_, PyAny>,
    previous_hashes: &[u64],
    hashes: &[u64],
) -> PyResult<Py<PyAny>> {
    let copy_module = py.import("copy")?;
    let merged = copy_module.call_method1("copy", (previous,))?;

    let mut merged_hashes = previous_hashes.to_vec();
    merged_hashes.extend_from_slice(hashes);
    merged.setattr("block_hashes", PyList::new(py, merged_hashes)?)?;

    let merged_tokens = PyList::empty(py);
    let previous_has_tokens = append_sequence_attr(py, &merged_tokens, previous, "token_ids")?;
    let event_has_tokens = append_sequence_attr(py, &merged_tokens, event, "token_ids")?;
    if previous_has_tokens || event_has_tokens {
        merged.setattr("token_ids", &merged_tokens)?;
    }

    let merged_extra = PyList::empty(py);
    let previous_has_extra = append_sequence_attr(py, &merged_extra, previous, "extra_keys")?;
    let event_has_extra = append_sequence_attr(py, &merged_extra, event, "extra_keys")?;
    if previous_has_extra || event_has_extra {
        merged.setattr("extra_keys", &merged_extra)?;
    }
    Ok(merged.unbind())
}

#[pymodule]
fn kv_event_selector(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RustKVEventSelector>()?;
    Ok(())
}
