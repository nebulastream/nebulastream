use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::BufferProvider;
use nes_source_runtime::{
    AsyncSource, Result, SOURCE_CREATION_FUNCTIONS, SourceCreateFn, SourceResult,
};
use nes_source_validation::{
    ConfigDefinition, ConfigOptions, ConfigOptionsType, ConfigOptionsTypeTag, ConfigValue, Error,
    SOURCE_VALIDATOR,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Binomial, Distribution, Normal};
use std::fmt::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, trace, warn};

const SOURCE_NAME: &str = "GENERATOR";

const SEED: &str = "SEED";
const GENERATOR_SCHEMA: &str = "GENERATOR_SCHEMA";
const GENERATOR_RATE_TYPE: &str = "GENERATOR_RATE_TYPE";
const GENERATOR_RATE_CONFIG: &str = "GENERATOR_RATE_CONFIG";
const STOP_WHEN_SEQUENCE_FINISHES: &str = "STOP_GENERATOR_WHEN_SEQUENCE_FINISHES";
const MAX_RUNTIME: &str = "MAX_RUNTIME";
const FLUSH_INTERVAL_MS: &str = "FLUSH_INTERVAL_MS";
const FAIL_AFTER: &str = "FAIL_AFTER";

fn validate_optional_duration(value: &ConfigValue) -> std::result::Result<(), Error> {
    let ConfigValue::Text(s) = value else {
        return Err("Expected a string".into());
    };
    if s.is_empty() {
        return Ok(());
    }
    duration_str::parse(s)?;
    Ok(())
}

fn parse_optional_duration(s: &str) -> std::result::Result<Option<Duration>, String> {
    if s.is_empty() {
        Ok(None)
    } else {
        duration_str::parse(s)
            .map(Some)
            .map_err(|e| format!("Invalid duration {s}: {e}"))
    }
}

#[distributed_slice(SOURCE_VALIDATOR)]
static GENERATOR_VALIDATOR: (&str, &[ConfigDefinition]) = (
    SOURCE_NAME,
    &[
        ConfigDefinition::with_type(GENERATOR_SCHEMA, ConfigOptionsTypeTag::Text),
        ConfigDefinition::with_default(SEED, ConfigOptionsType::Number(0)),
        ConfigDefinition::with_default(
            GENERATOR_RATE_TYPE,
            ConfigOptionsType::Text("FIXED"),
        ),
        ConfigDefinition::with_default(
            GENERATOR_RATE_CONFIG,
            ConfigOptionsType::Text("emit_rate 1000"),
        ),
        ConfigDefinition::with_default(
            STOP_WHEN_SEQUENCE_FINISHES,
            ConfigOptionsType::Text("NONE"),
        ),
        ConfigDefinition::with_default_and_check(
            MAX_RUNTIME,
            ConfigOptionsType::Text(""),
            &validate_optional_duration,
        ),
        ConfigDefinition::with_default(FLUSH_INTERVAL_MS, ConfigOptionsType::Number(10)),
        ConfigDefinition::with_default_and_check(
            FAIL_AFTER,
            ConfigOptionsType::Text(""),
            &validate_optional_duration,
        ),
    ],
);

#[distributed_slice(SOURCE_CREATION_FUNCTIONS)]
static GENERATOR_SOURCE: (&str, &SourceCreateFn) = (SOURCE_NAME, &create_generator_source);

// === Numeric Types ===

#[derive(Clone, Copy)]
enum NumericType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

fn parse_numeric_type(s: &str) -> std::result::Result<NumericType, String> {
    match s.to_uppercase().as_str() {
        "UINT8" => Ok(NumericType::U8),
        "UINT16" => Ok(NumericType::U16),
        "UINT32" => Ok(NumericType::U32),
        "UINT64" => Ok(NumericType::U64),
        "INT8" => Ok(NumericType::I8),
        "INT16" => Ok(NumericType::I16),
        "INT32" => Ok(NumericType::I32),
        "INT64" => Ok(NumericType::I64),
        "FLOAT32" => Ok(NumericType::F32),
        "FLOAT64" => Ok(NumericType::F64),
        _ => Err(format!("Unknown numeric type: {s}")),
    }
}

fn format_value(output: &mut String, data_type: NumericType, val: f64) {
    match data_type {
        NumericType::U8 => write!(output, "{}", val as u8).unwrap(),
        NumericType::U16 => write!(output, "{}", val as u16).unwrap(),
        NumericType::U32 => write!(output, "{}", val as u32).unwrap(),
        NumericType::U64 => write!(output, "{}", val as u64).unwrap(),
        NumericType::I8 => write!(output, "{}", val as i8 as i32).unwrap(),
        NumericType::I16 => write!(output, "{}", val as i16).unwrap(),
        NumericType::I32 => write!(output, "{}", val as i32).unwrap(),
        NumericType::I64 => write!(output, "{}", val as i64).unwrap(),
        NumericType::F32 => write!(output, "{}", val as f32).unwrap(),
        NumericType::F64 => write!(output, "{}", val).unwrap(),
    }
}

fn is_float_type(t: NumericType) -> bool {
    matches!(t, NumericType::F32 | NumericType::F64)
}

// === Generator Fields ===

#[derive(Clone, Copy, PartialEq, Eq)]
enum GeneratorStop {
    All,
    One,
    None,
}

fn parse_generator_stop(s: &str) -> std::result::Result<GeneratorStop, String> {
    match s.to_uppercase().as_str() {
        "ALL" => Ok(GeneratorStop::All),
        "ONE" => Ok(GeneratorStop::One),
        "NONE" => Ok(GeneratorStop::None),
        _ => Err(format!("Unknown stop behavior: {s}")),
    }
}

struct SequenceField {
    data_type: NumericType,
    position: f64,
    end: f64,
    step: f64,
    stopped: bool,
}

impl SequenceField {
    fn parse(parts: &[&str]) -> std::result::Result<Self, String> {
        if parts.len() != 5 {
            return Err(format!(
                "SEQUENCE requires 5 parameters (SEQUENCE type start end step), got {}",
                parts.len()
            ));
        }
        let data_type = parse_numeric_type(parts[1])?;
        let start: f64 = parts[2]
            .parse()
            .map_err(|_| format!("Invalid start: {}", parts[2]))?;
        let end: f64 = parts[3]
            .parse()
            .map_err(|_| format!("Invalid end: {}", parts[3]))?;
        let step: f64 = parts[4]
            .parse()
            .map_err(|_| format!("Invalid step: {}", parts[4]))?;
        Ok(SequenceField {
            data_type,
            position: start,
            end,
            step,
            stopped: false,
        })
    }

    fn generate(&mut self, output: &mut String) {
        format_value(output, self.data_type, self.position);
        if self.position < self.end {
            self.position += self.step;
        }
        if self.position >= self.end {
            self.stopped = true;
        }
    }

}

enum DistributionKind {
    NormalDist(Normal<f64>),
    BinomialDist(Binomial),
}

struct NormalDistributionField {
    data_type: NumericType,
    dist: DistributionKind,
}

impl NormalDistributionField {
    fn parse(parts: &[&str]) -> std::result::Result<Self, String> {
        if parts.len() < 4 {
            return Err(format!(
                "NORMAL_DISTRIBUTION requires 4 parameters, got {}",
                parts.len()
            ));
        }
        let data_type = parse_numeric_type(parts[1])?;
        let mean: f64 = parts[2]
            .parse()
            .map_err(|_| format!("Invalid mean: {}", parts[2]))?;
        let stddev: f64 = parts[3]
            .parse()
            .map_err(|_| format!("Invalid stddev: {}", parts[3]))?;

        let dist = if is_float_type(data_type) {
            DistributionKind::NormalDist(
                Normal::new(mean, stddev)
                    .map_err(|e| format!("Invalid normal distribution params: {e}"))?,
            )
        } else {
            DistributionKind::BinomialDist(
                Binomial::new(mean as u64, stddev)
                    .map_err(|e| format!("Invalid binomial distribution params: {e}"))?,
            )
        };

        Ok(NormalDistributionField { data_type, dist })
    }

    fn generate(&mut self, output: &mut String, rng: &mut StdRng) {
        match &self.dist {
            DistributionKind::NormalDist(dist) => {
                let val: f64 = dist.sample(rng);
                format_value(output, self.data_type, val);
            }
            DistributionKind::BinomialDist(dist) => {
                let val: u64 = dist.sample(rng);
                format_value(output, self.data_type, val as f64);
            }
        }
    }
}

struct WordListField {
    words: Vec<String>,
}

impl WordListField {
    fn parse(parts: &[&str]) -> std::result::Result<Self, String> {
        if parts.len() != 2 {
            return Err(format!(
                "WORDLIST requires 2 parameters (WORDLIST path), got {}",
                parts.len()
            ));
        }
        let path = parts[1];
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Cannot read wordlist {path}: {e}"))?;
        let words: Vec<String> = content.lines().filter(|l| !l.is_empty()).map(String::from).collect();
        if words.is_empty() {
            return Err(format!("Wordlist file {path} contains no words"));
        }
        Ok(WordListField { words })
    }

    fn generate(&self, output: &mut String, rng: &mut StdRng) {
        let idx = rng.gen_range(0..self.words.len());
        output.push_str(&self.words[idx]);
    }
}

struct RandomStrField {
    min_length: usize,
    max_length: usize,
}

const BASE64_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

impl RandomStrField {
    fn parse(parts: &[&str]) -> std::result::Result<Self, String> {
        if parts.len() != 3 {
            return Err(format!(
                "RANDOMSTR requires 3 parameters (RANDOMSTR min max), got {}",
                parts.len()
            ));
        }
        let min_length: usize = parts[1]
            .parse()
            .map_err(|_| format!("Invalid min_length: {}", parts[1]))?;
        let max_length: usize = parts[2]
            .parse()
            .map_err(|_| format!("Invalid max_length: {}", parts[2]))?;
        if min_length > max_length {
            return Err(format!(
                "RANDOMSTR min_length ({min_length}) > max_length ({max_length})"
            ));
        }
        Ok(RandomStrField {
            min_length,
            max_length,
        })
    }

    fn generate(&self, output: &mut String, rng: &mut StdRng) {
        let length = if self.min_length == self.max_length {
            self.min_length
        } else {
            rng.gen_range(self.min_length..=self.max_length)
        };
        for _ in 0..length {
            let idx = rng.gen_range(0..BASE64_ALPHABET.len());
            output.push(BASE64_ALPHABET[idx] as char);
        }
    }
}

enum GeneratorField {
    Sequence(SequenceField),
    NormalDistribution(NormalDistributionField),
    WordList(WordListField),
    RandomStr(RandomStrField),
}

impl GeneratorField {
    fn generate(&mut self, output: &mut String, rng: &mut StdRng) {
        match self {
            GeneratorField::Sequence(f) => f.generate(output),
            GeneratorField::NormalDistribution(f) => f.generate(output, rng),
            GeneratorField::WordList(f) => f.generate(output, rng),
            GeneratorField::RandomStr(f) => f.generate(output, rng),
        }
    }

    fn is_stoppable(&self) -> bool {
        matches!(self, GeneratorField::Sequence(_))
    }

    fn is_stopped(&self) -> bool {
        match self {
            GeneratorField::Sequence(f) => f.stopped,
            _ => false,
        }
    }
}

// === Generator ===

struct Generator {
    fields: Vec<GeneratorField>,
    stop_behavior: GeneratorStop,
    rng: StdRng,
    num_stoppable_fields: usize,
    num_stopped_fields: usize,
}

impl Generator {
    fn new(seed: u64, stop_behavior: GeneratorStop, schema: &str) -> std::result::Result<Self, String> {
        let mut generator = Generator {
            fields: Vec::new(),
            stop_behavior,
            rng: StdRng::seed_from_u64(seed),
            num_stoppable_fields: 0,
            num_stopped_fields: 0,
        };
        generator.parse_schema(schema)?;
        Ok(generator)
    }

    fn parse_schema(&mut self, schema: &str) -> std::result::Result<(), String> {
        if schema.is_empty() {
            return Err("Generator schema cannot be empty".into());
        }
        for line in schema.split([',', '\n']) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            self.parse_field(line)?;
        }
        if self.fields.is_empty() {
            return Err("Generator schema produced no fields".into());
        }
        Ok(())
    }

    fn parse_field(&mut self, line: &str) -> std::result::Result<(), String> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty field line".into());
        }
        let field = match parts[0].to_uppercase().as_str() {
            "SEQUENCE" => GeneratorField::Sequence(SequenceField::parse(&parts)?),
            "NORMAL_DISTRIBUTION" => {
                GeneratorField::NormalDistribution(NormalDistributionField::parse(&parts)?)
            }
            "WORDLIST" => GeneratorField::WordList(WordListField::parse(&parts)?),
            "RANDOMSTR" => GeneratorField::RandomStr(RandomStrField::parse(&parts)?),
            other => return Err(format!("Unknown field type: {other}")),
        };
        if field.is_stoppable() {
            self.num_stoppable_fields += 1;
        }
        self.fields.push(field);
        Ok(())
    }

    fn generate_tuple(&mut self, output: &mut String) {
        let rng = &mut self.rng;
        for (i, field) in self.fields.iter_mut().enumerate() {
            if i > 0 {
                output.push(',');
            }
            let was_stopped = field.is_stopped();
            field.generate(output, rng);
            if !was_stopped && field.is_stopped() {
                self.num_stopped_fields += 1;
            }
        }
        output.push('\n');
    }

    fn should_stop(&self) -> bool {
        match self.stop_behavior {
            GeneratorStop::All => {
                self.num_stoppable_fields > 0
                    && self.num_stoppable_fields == self.num_stopped_fields
            }
            GeneratorStop::One => self.num_stopped_fields >= 1,
            GeneratorStop::None => false,
        }
    }
}

// === Rate Control ===

trait Rate: Send {
    fn calc_tuples_for_interval(&self, start: SystemTime, end: SystemTime) -> u64;
}

struct FixedRate {
    emit_rate: f64,
}

impl FixedRate {
    fn parse(config: &str) -> std::result::Result<Self, String> {
        let parts: Vec<&str> = config.split_whitespace().collect();
        if parts.len() == 2 && parts[0].eq_ignore_ascii_case("emit_rate") {
            let emit_rate: f64 = parts[1]
                .parse()
                .map_err(|_| format!("Invalid emit_rate: {}", parts[1]))?;
            Ok(FixedRate { emit_rate })
        } else {
            Err(format!("Invalid fixed rate config: {config}"))
        }
    }
}

impl Rate for FixedRate {
    fn calc_tuples_for_interval(&self, start: SystemTime, end: SystemTime) -> u64 {
        let duration = end.duration_since(start).unwrap_or_default();
        (self.emit_rate * duration.as_secs_f64()) as u64
    }
}

struct SinusRate {
    amplitude: f64,
    frequency: f64,
}

impl SinusRate {
    fn parse(config: &str) -> std::result::Result<Self, String> {
        let mut amplitude = None;
        let mut frequency = None;

        for param in config.split([',', '\n']) {
            let param = param.trim();
            let parts: Vec<&str> = param.split_whitespace().collect();
            if parts.len() == 2 {
                if parts[0].eq_ignore_ascii_case("amplitude") {
                    amplitude = Some(
                        parts[1]
                            .parse::<f64>()
                            .map_err(|_| format!("Invalid amplitude: {}", parts[1]))?,
                    );
                } else if parts[0].eq_ignore_ascii_case("frequency") {
                    frequency = Some(
                        parts[1]
                            .parse::<f64>()
                            .map_err(|_| format!("Invalid frequency: {}", parts[1]))?,
                    );
                }
            }
        }

        match (amplitude, frequency) {
            (Some(a), Some(f)) => Ok(SinusRate {
                amplitude: a,
                frequency: f,
            }),
            _ => Err(format!("Invalid sinus rate config: {config}")),
        }
    }
}

impl Rate for SinusRate {
    fn calc_tuples_for_interval(&self, start: SystemTime, end: SystemTime) -> u64 {
        let start_secs = start.duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
        let end_secs = end.duration_since(UNIX_EPOCH).unwrap().as_secs_f64();

        let first_cos = (self.frequency * start_secs).cos();
        let second_cos = self.frequency * (start_secs - end_secs);
        let third_cos = (end_secs * self.frequency).cos();
        let result =
            (self.amplitude * (first_cos - second_cos - third_cos)) / (2.0 * self.frequency);
        result.max(0.0) as u64
    }
}

fn create_rate(rate_type: &str, rate_config: &str) -> std::result::Result<Box<dyn Rate>, String> {
    match rate_type.to_uppercase().as_str() {
        "FIXED" => Ok(Box::new(FixedRate::parse(rate_config)?)),
        "SINUS" => Ok(Box::new(SinusRate::parse(rate_config)?)),
        _ => Err(format!("Unknown rate type: {rate_type}")),
    }
}

// === Generator Source ===

struct GeneratorSource {
    generator: Generator,
    rate: Box<dyn Rate>,
    flush_interval: Duration,
    max_runtime: Option<Duration>,
    fail_after: Option<Duration>,
    start_time: Option<Instant>,
    interval_start: Option<SystemTime>,
    orphan_tuples: String,
    generated_buffers: u64,
}

#[async_trait]
impl AsyncSource for GeneratorSource {
    async fn start(&mut self) -> Result<()> {
        if self.fail_after == Some(Duration::ZERO) {
            return Err("GeneratorSource configured to fail at start".into());
        }
        self.start_time = Some(Instant::now());
        self.interval_start = Some(SystemTime::now());
        info!("Opening GeneratorSource");
        Ok(())
    }

    async fn receive(&mut self, provider: &mut BufferProvider) -> Result<SourceResult> {
        let start_time = self.start_time.unwrap();

        if let Some(fail_after) = self.fail_after {
            if start_time.elapsed() >= fail_after {
                return Err(format!(
                    "GeneratorSource configured to fail after {:?}",
                    fail_after
                ));
            }
        }

        if let Some(max_rt) = self.max_runtime {
            if start_time.elapsed() >= max_rt {
                info!("Reached max runtime, stopping source");
                return Ok(SourceResult::EoS);
            }
        }

        let interval_start = self.interval_start.unwrap();
        let mut num_tuples: u64 = 0;
        let mut no_intervals: u64 = 1;

        while num_tuples == 0 {
            let end = interval_start + self.flush_interval * no_intervals as u32;
            num_tuples = self.rate.calc_tuples_for_interval(interval_start, end);
            trace!("numberOfTuplesToGenerate: {}", num_tuples);
            if num_tuples == 0 {
                tokio::time::sleep(self.flush_interval).await;
                no_intervals += 1;
            }
        }

        let mut buffer = provider.allocate().await;
        let buf_size = buffer.get_data_mut().len();

        let mut output = String::with_capacity(buf_size);
        let mut written_bytes: usize = 0;
        let mut cur_tuple_count: u64 = 0;

        while cur_tuple_count < num_tuples {
            if self.generator.should_stop() {
                break;
            }

            let before = output.len();
            if !self.orphan_tuples.is_empty() {
                output.push_str(&self.orphan_tuples);
                self.orphan_tuples.clear();
            }
            self.generator.generate_tuple(&mut output);
            let inserted_bytes = output.len() - before;

            if written_bytes + inserted_bytes > buf_size {
                self.orphan_tuples = output[written_bytes..].to_string();
                warn!(
                    "Not all required tuples fit into buffer of size {}. {} bytes left over",
                    buf_size,
                    self.orphan_tuples.len()
                );
                break;
            }
            written_bytes += inserted_bytes;
            cur_tuple_count += 1;
        }

        if cur_tuple_count == 0 {
            return Ok(SourceResult::EoS);
        }

        buffer.get_data_mut()[..written_bytes].copy_from_slice(&output.as_bytes()[..written_bytes]);
        self.generated_buffers += 1;
        trace!("Wrote {} bytes", written_bytes);

        let target_duration = self.flush_interval * no_intervals as u32;
        let elapsed_since_interval = self
            .interval_start
            .unwrap()
            .elapsed()
            .unwrap_or_default();

        if elapsed_since_interval < target_duration {
            let sleep_duration = target_duration - elapsed_since_interval;
            tokio::time::sleep(sleep_duration).await;
        } else {
            warn!(
                "Cannot produce all required tuples in flush interval of {:?}",
                target_duration
            );
        }

        self.interval_start = Some(SystemTime::now());
        Ok(SourceResult::Data(buffer, written_bytes))
    }

    async fn stop(&mut self) -> Result<()> {
        if self.generated_buffers == 0 {
            warn!(
                "Generated {} buffers. Closing GeneratorSource.",
                self.generated_buffers
            );
        } else {
            info!(
                "Generated {} buffers. Closing GeneratorSource.",
                self.generated_buffers
            );
        }
        Ok(())
    }
}

impl TryFrom<&ConfigOptions> for GeneratorSource {
    type Error = String;

    fn try_from(config: &ConfigOptions) -> std::result::Result<Self, String> {
        let seed: u64 = config
            .get(SEED)
            .unwrap()
            .parse()
            .map_err(|_| "Invalid seed")?;
        let schema = config.get(GENERATOR_SCHEMA).ok_or("Missing generator_schema")?;
        let rate_type = config.get(GENERATOR_RATE_TYPE).unwrap();
        let rate_config = config.get(GENERATOR_RATE_CONFIG).unwrap();
        let stop_str = config.get(STOP_WHEN_SEQUENCE_FINISHES).unwrap();
        let max_runtime = parse_optional_duration(config.get(MAX_RUNTIME).unwrap())?;
        let flush_interval_ms: u64 = config
            .get(FLUSH_INTERVAL_MS)
            .unwrap()
            .parse()
            .map_err(|_| "Invalid flush_interval_ms")?;
        let fail_after = parse_optional_duration(config.get(FAIL_AFTER).unwrap())?;

        let stop_behavior = parse_generator_stop(stop_str)?;
        let generator = Generator::new(seed, stop_behavior, schema)?;
        let rate = create_rate(rate_type, rate_config)?;

        Ok(GeneratorSource {
            generator,
            rate,
            flush_interval: Duration::from_millis(flush_interval_ms),
            max_runtime,
            fail_after,
            start_time: None,
            interval_start: None,
            orphan_tuples: String::new(),
            generated_buffers: 0,
        })
    }
}

fn create_generator_source(config_options: &ConfigOptions) -> Box<dyn AsyncSource + Send> {
    Box::new(
        GeneratorSource::try_from(config_options)
            .expect("Generator source config should have been validated"),
    )
}
