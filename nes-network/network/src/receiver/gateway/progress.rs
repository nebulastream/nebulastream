use std::cmp::max;
use std::collections::{BTreeMap, HashMap, VecDeque};
use crate::protocol::TupleBuffer;
use fixedbitset::FixedBitSet;
use crate::receiver::gateway::{ChunkNumber, Epoch, OriginId, SequenceNumber};


pub struct SNDState {
    origins : HashMap<OriginId, OriginSNDState>,
}

pub enum SNDResult {
    PassThroughSingle(TupleBuffer, SequenceNumber),
    Buffered(TupleBuffer, SequenceNumber),
    PassThroughBuffered(TupleBuffer, VecDeque<TupleBuffer>, SequenceNumber),
    Deduplicated(TupleBuffer),
}

impl SNDState {
    pub fn new() -> Self {
        Self {
            origins: HashMap::new(),
        }
    }

    pub fn restore_watermarks(&mut self, watermarks: HashMap<OriginId, SequenceNumber>) {
        for (origin_id, watermark) in watermarks {
            let origin_state = self.origins.entry(origin_id).or_insert_with(OriginSNDState::new);
            origin_state.lwm = watermark;
            origin_state.hwm = watermark;
        }
    }

    pub fn collect_hwms(&self) -> HashMap<OriginId, SequenceNumber> {
        self.origins
            .iter()
            .map(|(origin_id, state)| (*origin_id, state.hwm))
            .collect()
    }

    pub fn process(&mut self, buffer: TupleBuffer) -> SNDResult {
        // get or create record for given sequence number
        let origin_state = self.origins
            .entry(buffer.origin_id)
            .or_insert_with(OriginSNDState::new);

        // Low watermark filter
        if buffer.sequence_number <= origin_state.lwm {
            return SNDResult::Deduplicated(buffer);
        }

        // Progress high watermark
        origin_state.hwm = max(origin_state.hwm, buffer.sequence_number);


        let sn_record = origin_state.sequence_numbers
            .entry(buffer.sequence_number)
            .or_insert(SNRecord::new(buffer.origin_epoch, buffer.predecessor));

        // Finalized (but not yet GC'ed) sequence number filter
        if sn_record.finalized {
            return SNDResult::Deduplicated(buffer);
        }

        // short circuit for unchunked sequence numbers
        if buffer.last_chunk && buffer.chunk_number == 1 {
            sn_record.finalized = true;
            origin_state.garbage_collect();
            return SNDResult::PassThroughSingle(buffer, origin_state.lwm);
        }

        // Restart record when a new epoch is detected
        // TODO: confirm that epochs will never arrive out of order for a single SN
        if sn_record.epoch < buffer.origin_epoch {
            sn_record.reset_to(buffer.origin_epoch, buffer.predecessor);
        }

        // Deduplicate chunks of same epoch
        let chunk_nr = usize::try_from(buffer.chunk_number).expect("need to change this if chunk numbers ever surpass usize");
        if sn_record.present_chunks.contains(chunk_nr) {
            return SNDResult::Deduplicated(buffer)
        }

        // update SN record
        if buffer.last_chunk {
            sn_record.max_chunk_nr = buffer.chunk_number;
        }
        sn_record.present_chunks.grow_and_insert(chunk_nr);
        sn_record.num_chunks += 1;
        sn_record.buffers.push_back(buffer.clone());

        // are all chunks there?
        if sn_record.num_chunks >= sn_record.max_chunk_nr {
            sn_record.finalized = true;
            let buffers = std::mem::take(&mut sn_record.buffers);
            origin_state.garbage_collect();
            SNDResult::PassThroughBuffered(buffer, buffers, origin_state.lwm)
        } else {
            SNDResult::Buffered(buffer, origin_state.lwm)
        }
    }
}


struct OriginSNDState {
    sequence_numbers: BTreeMap<SequenceNumber, SNRecord>,
    lwm: SequenceNumber, // = low watermark = highest sequence number up until which all sequence numbers have arrived completely
    hwm: SequenceNumber, // = high watermark = highest seen sequence number
}

impl OriginSNDState {
    fn new() -> Self {
        Self {
            sequence_numbers: BTreeMap::new(),
            lwm: 0,
            hwm: 0,
        }
    }

    fn garbage_collect(&mut self) {
        while let Some((&sequence_number, record)) = self.sequence_numbers.first_key_value() {
            if !record.finalized {
                break;
            }
            if record.predecessor_sn <= self.lwm {
                self.lwm = sequence_number;
                self.sequence_numbers.pop_first();
            } else {
                break;
            }
        }
    }
}

struct SNRecord {
    epoch: Epoch,
    buffers: VecDeque<TupleBuffer>,
    max_chunk_nr: ChunkNumber,
    num_chunks: u64,
    present_chunks: FixedBitSet,
    finalized: bool,
    predecessor_sn: SequenceNumber,
}

impl SNRecord {
    fn new(epoch: Epoch, predecessor_sn: SequenceNumber) -> Self {
        Self {
            epoch,
            buffers: VecDeque::new(),
            max_chunk_nr: ChunkNumber::MAX,
            num_chunks: 0,
            present_chunks: FixedBitSet::with_capacity(64),
            finalized: false,
            predecessor_sn
        }
    }
    fn reset_to(&mut self, new_epoch : Epoch, predecessor_sn: SequenceNumber) {
        self.epoch = new_epoch;
        self.buffers.clear();
        self.present_chunks.clear();
        self.max_chunk_nr = ChunkNumber::MAX;
        self.num_chunks = 0;
        self.predecessor_sn = predecessor_sn;
    }
}