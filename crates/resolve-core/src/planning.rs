use crate::domain::{AcceptanceRef, ClaimEpoch, CommitmentId, Prerequisite, StructuralChange};
use crate::error::DomainError;
use std::collections::{BTreeMap, BTreeSet};

/// The only structural mutations a worker may propose in R0.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StructuralProposal {
    Decompose {
        target: CommitmentId,
        epoch: ClaimEpoch,
        children: Vec<NewCommitment>,
        replacement_terminal_id: CommitmentId,
    },
    AddPrerequisite {
        target: CommitmentId,
        prerequisite: Prerequisite,
        epoch: ClaimEpoch,
    },
    Abandon {
        target: CommitmentId,
        epoch: ClaimEpoch,
        rationale: String,
    },
}

/// The child description supplied by a decomposition proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewCommitment {
    commitment_id: CommitmentId,
    description: String,
    prerequisites: Vec<Prerequisite>,
    acceptance_refs: Vec<AcceptanceRef>,
}

impl NewCommitment {
    pub fn new(
        commitment_id: CommitmentId,
        description: impl Into<String>,
        prerequisites: Vec<Prerequisite>,
        acceptance_refs: Vec<AcceptanceRef>,
    ) -> Result<Self, DomainError> {
        let description = description.into();
        if description.trim().is_empty() {
            return Err(DomainError::EmptyDescription);
        }
        if has_duplicate_prerequisites(&prerequisites) {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "child prerequisites must be unique",
            });
        }
        Ok(Self {
            commitment_id,
            description,
            prerequisites,
            acceptance_refs,
        })
    }

    pub fn commitment_id(&self) -> &CommitmentId {
        &self.commitment_id
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn prerequisites(&self) -> &[Prerequisite] {
        &self.prerequisites
    }

    pub fn acceptance_refs(&self) -> &[AcceptanceRef] {
        &self.acceptance_refs
    }
}

/// Validate the proposal-local rules that do not require persistence.
pub fn validate_proposal_shape(proposal: &StructuralProposal) -> Result<(), DomainError> {
    match proposal {
        StructuralProposal::Decompose {
            target,
            children,
            replacement_terminal_id,
            ..
        } => {
            if children.is_empty() {
                return Err(DomainError::InvalidStructuralProposal {
                    reason: "decomposition must contain at least one child",
                });
            }
            let mut ids = BTreeSet::new();
            for child in children {
                if !ids.insert(child.commitment_id().clone()) {
                    return Err(DomainError::InvalidStructuralProposal {
                        reason: "decomposition child IDs must be unique",
                    });
                }
            }
            if target == replacement_terminal_id {
                return Err(DomainError::InvalidStructuralProposal {
                    reason: "replacement terminal must be a new child",
                });
            }
            if !ids.contains(replacement_terminal_id) {
                return Err(DomainError::InvalidStructuralProposal {
                    reason: "replacement terminal must identify one proposed child",
                });
            }
            validate_child_prerequisites(children)
        }
        StructuralProposal::AddPrerequisite { prerequisite, .. } => {
            if let Prerequisite::Commitment(id) = prerequisite
                && id.as_ref().trim().is_empty()
            {
                return Err(DomainError::InvalidStructuralProposal {
                    reason: "commitment prerequisite ID must not be empty",
                });
            }
            Ok(())
        }
        StructuralProposal::Abandon { rationale, .. } => {
            if rationale.trim().is_empty() {
                return Err(DomainError::InvalidStructuralProposal {
                    reason: "abandon rationale must not be empty",
                });
            }
            Ok(())
        }
    }
}

/// Prove that a typed commitment dependency graph is acyclic.
pub fn validate_acyclic(
    graph: &BTreeMap<CommitmentId, BTreeSet<CommitmentId>>,
) -> Result<(), DomainError> {
    let mut colours = BTreeMap::new();
    for node in graph.keys() {
        if !matches!(colours.get(node), Some(NodeColour::Black)) {
            visit(node, graph, &mut colours)?;
        }
    }
    Ok(())
}

fn visit(
    node: &CommitmentId,
    graph: &BTreeMap<CommitmentId, BTreeSet<CommitmentId>>,
    colours: &mut BTreeMap<CommitmentId, NodeColour>,
) -> Result<(), DomainError> {
    match colours.get(node) {
        Some(NodeColour::Grey) => return Err(DomainError::DependencyCycle),
        Some(NodeColour::Black) => return Ok(()),
        None => {}
    }
    colours.insert(node.clone(), NodeColour::Grey);
    if let Some(dependencies) = graph.get(node) {
        for dependency in dependencies {
            visit(dependency, graph, colours)?;
        }
    }
    colours.insert(node.clone(), NodeColour::Black);
    Ok(())
}

fn validate_child_prerequisites(children: &[NewCommitment]) -> Result<(), DomainError> {
    for child in children {
        if has_duplicate_prerequisites(child.prerequisites()) {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "child prerequisites must be unique",
            });
        }
    }
    Ok(())
}

fn has_duplicate_prerequisites(prerequisites: &[Prerequisite]) -> bool {
    prerequisites
        .iter()
        .enumerate()
        .any(|(index, prerequisite)| prerequisites[..index].contains(prerequisite))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodeColour {
    Grey,
    Black,
}

/// Keep the event's structural evidence closed and typed at the domain edge.
pub fn change_for_decomposition(
    children: Vec<CommitmentId>,
    replacement_terminal_id: CommitmentId,
) -> StructuralChange {
    StructuralChange::Decomposed {
        children,
        replacement_terminal_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TethersContractRef;

    fn commitment_id(value: &str) -> CommitmentId {
        CommitmentId::try_new(value).expect("test identifier is valid")
    }

    fn child(value: &str, prerequisites: Vec<Prerequisite>) -> NewCommitment {
        NewCommitment::new(commitment_id(value), value, prerequisites, Vec::new())
            .expect("test child is valid")
    }

    #[test]
    fn acyclic_graph_is_accepted_deterministically() {
        let mut graph = BTreeMap::new();
        graph.insert(commitment_id("a"), BTreeSet::from([commitment_id("b")]));
        graph.insert(commitment_id("b"), BTreeSet::new());
        assert!(validate_acyclic(&graph).is_ok());
    }

    #[test]
    fn direct_and_indirect_cycles_are_rejected() {
        let mut direct = BTreeMap::new();
        direct.insert(commitment_id("a"), BTreeSet::from([commitment_id("a")]));
        assert_eq!(
            validate_acyclic(&direct).expect_err("self dependency is a cycle"),
            DomainError::DependencyCycle
        );

        let mut indirect = BTreeMap::new();
        indirect.insert(commitment_id("a"), BTreeSet::from([commitment_id("b")]));
        indirect.insert(commitment_id("b"), BTreeSet::from([commitment_id("c")]));
        indirect.insert(commitment_id("c"), BTreeSet::from([commitment_id("a")]));
        assert_eq!(
            validate_acyclic(&indirect).expect_err("indirect cycle is rejected"),
            DomainError::DependencyCycle
        );
    }

    #[test]
    fn proposal_shape_rejects_empty_duplicate_and_unknown_terminal() {
        let target = commitment_id("target");
        let empty = StructuralProposal::Decompose {
            target: target.clone(),
            epoch: ClaimEpoch::initial(),
            children: Vec::new(),
            replacement_terminal_id: commitment_id("child"),
        };
        assert!(matches!(
            validate_proposal_shape(&empty),
            Err(DomainError::InvalidStructuralProposal { .. })
        ));

        let duplicate = StructuralProposal::Decompose {
            target: target.clone(),
            epoch: ClaimEpoch::initial(),
            children: vec![child("child", Vec::new()), child("child", Vec::new())],
            replacement_terminal_id: commitment_id("child"),
        };
        assert!(matches!(
            validate_proposal_shape(&duplicate),
            Err(DomainError::InvalidStructuralProposal { .. })
        ));

        let unknown = StructuralProposal::Decompose {
            target,
            epoch: ClaimEpoch::initial(),
            children: vec![child("child", Vec::new())],
            replacement_terminal_id: commitment_id("missing"),
        };
        assert!(matches!(
            validate_proposal_shape(&unknown),
            Err(DomainError::InvalidStructuralProposal { .. })
        ));
    }

    #[test]
    fn typed_non_commitment_prerequisite_has_no_graph_edge() {
        let prerequisite = Prerequisite::TethersVerification(
            TethersContractRef::try_new("contract-1").expect("contract is valid"),
        );
        assert!(!matches!(prerequisite, Prerequisite::Commitment(_)));
    }

    #[test]
    fn empty_abandon_rationale_is_rejected() {
        let proposal = StructuralProposal::Abandon {
            target: commitment_id("target"),
            epoch: ClaimEpoch::initial(),
            rationale: " \n ".to_owned(),
        };
        assert!(matches!(
            validate_proposal_shape(&proposal),
            Err(DomainError::InvalidStructuralProposal { .. })
        ));
    }
}
