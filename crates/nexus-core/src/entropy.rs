#[derive(Debug, Clone, Copy)]
pub struct EntropyThresholds {
    pub warning: f64,
    pub degradation: f64,
    pub halt: f64,
    pub circuit_breaker: f64,
}

impl Default for EntropyThresholds {
    fn default() -> Self {
        Self {
            warning: 0.3,
            degradation: 0.5,
            halt: 0.7,
            circuit_breaker: 0.85,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EntropySignals {
    pub retry_rate: f64,
    pub worker_failure_rate: f64,
    pub validation_divergence: f64,
}

impl EntropySignals {
    pub fn new(retry_rate: f64, worker_failure_rate: f64, validation_divergence: f64) -> Self {
        Self {
            retry_rate,
            worker_failure_rate,
            validation_divergence,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EntropyController {
    pub thresholds: EntropyThresholds,
}

impl EntropyController {
    pub fn new(thresholds: EntropyThresholds) -> Self {
        Self { thresholds }
    }

    pub fn calculate(&self, signals: &EntropySignals) -> f64 {
        let retry_score = signals.retry_rate.min(1.0);
        let failure_score = signals.worker_failure_rate.min(1.0);
        let divergence_score = signals.validation_divergence.min(1.0);

        (retry_score * 0.4 + failure_score * 0.4 + divergence_score * 0.2).min(1.0)
    }

    pub fn respond(&self, score: f64) -> Vec<EntropyAction> {
        if score >= self.thresholds.circuit_breaker {
            vec![
                EntropyAction::HaltExecution,
                EntropyAction::LockNewTasks,
                EntropyAction::AlertOperator,
            ]
        } else if score >= self.thresholds.halt {
            vec![
                EntropyAction::ReduceParallelism,
                EntropyAction::TriggerHumanReview,
                EntropyAction::SnapshotCheckpoint,
            ]
        } else if score >= self.thresholds.degradation {
            vec![
                EntropyAction::FreezeAdaptation,
                EntropyAction::IncreaseValidation,
            ]
        } else if score >= self.thresholds.warning {
            vec![EntropyAction::IncreaseSampling, EntropyAction::LogWarning]
        } else {
            vec![]
        }
    }

    pub fn get_entropy_level(&self, score: f64) -> EntropyLevel {
        if score >= self.thresholds.circuit_breaker {
            EntropyLevel::CircuitBreaker
        } else if score >= self.thresholds.halt {
            EntropyLevel::Halt
        } else if score >= self.thresholds.degradation {
            EntropyLevel::Degradation
        } else if score >= self.thresholds.warning {
            EntropyLevel::Warning
        } else {
            EntropyLevel::Normal
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntropyLevel {
    Normal,
    Warning,
    Degradation,
    Halt,
    CircuitBreaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntropyAction {
    IncreaseSampling,
    LogWarning,
    FreezeAdaptation,
    IncreaseValidation,
    ReduceParallelism,
    TriggerHumanReview,
    SnapshotCheckpoint,
    HaltExecution,
    LockNewTasks,
    AlertOperator,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entropy_normal() {
        let controller = EntropyController::default();
        let signals = EntropySignals {
            retry_rate: 0.0,
            worker_failure_rate: 0.0,
            validation_divergence: 0.0,
        };
        let score = controller.calculate(&signals);
        assert_eq!(score, 0.0);
        assert_eq!(controller.get_entropy_level(score), EntropyLevel::Normal);
        assert!(controller.respond(score).is_empty());
    }

    #[test]
    fn test_entropy_warning() {
        let controller = EntropyController::default();
        let signals = EntropySignals {
            retry_rate: 0.8,
            worker_failure_rate: 0.1,
            validation_divergence: 0.1,
        };
        let score = controller.calculate(&signals);
        // 0.8*0.4 + 0.1*0.4 + 0.1*0.2 = 0.32 + 0.04 + 0.02 = 0.38
        assert!(score >= controller.thresholds.warning);
        let actions = controller.respond(score);
        assert!(actions.contains(&EntropyAction::LogWarning));
    }

    #[test]
    fn test_entropy_circuit_breaker() {
        let controller = EntropyController::default();
        let signals = EntropySignals {
            retry_rate: 1.0,
            worker_failure_rate: 1.0,
            validation_divergence: 1.0,
        };
        let score = controller.calculate(&signals);
        assert_eq!(score, 1.0);
        assert_eq!(
            controller.get_entropy_level(score),
            EntropyLevel::CircuitBreaker
        );
        let actions = controller.respond(score);
        assert!(actions.contains(&EntropyAction::HaltExecution));
    }

    #[test]
    fn test_entropy_scoring_weights() {
        let controller = EntropyController::default();
        let signals = EntropySignals {
            retry_rate: 1.0,
            worker_failure_rate: 0.0,
            validation_divergence: 0.0,
        };
        let score = controller.calculate(&signals);
        assert!(
            (score - 0.4).abs() < 0.001,
            "Retry rate weight should be 0.4, got {}",
            score
        );
    }
}
