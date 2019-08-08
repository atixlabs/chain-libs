use crate::{
    config::ConfigParam,
    leadership::bft::LeaderId,
    update::{
        SignedUpdateProposal, SignedUpdateVote, UpdateProposal, UpdateProposalId,
        UpdateProposalWithProposer, UpdateVote,
    },
};
use chain_crypto::{Ed25519Extended, SecretKey};

pub fn build_proposal(
    proposer_id: LeaderId,
    proposer_secret_key: SecretKey<Ed25519Extended>,
    config_params: Vec<ConfigParam>,
) -> SignedUpdateProposal {
    
    //create proposal
    let mut update_proposal = UpdateProposal::new();

    for config_param in config_params
    {
        update_proposal.changes.push(config_param);
    }

    //add proposer
    let proposal_signature = update_proposal.make_certificate(&proposer_secret_key);
    let update_proposal_with_proposer = UpdateProposalWithProposer {
        proposal: update_proposal,
        proposer_id: proposer_id.clone(),
    };

    //sign proposal
    SignedUpdateProposal {
        proposal: update_proposal_with_proposer,
        signature: proposal_signature,
    }
}

pub fn build_vote(
    proposal_id: UpdateProposalId,
    leader_id: LeaderId,
    leader_secret_key: SecretKey<Ed25519Extended>,
) -> SignedUpdateVote {
    let update_vote = UpdateVote {
        proposal_id: proposal_id.clone(),
        voter_id: leader_id.clone(),
    };
    let vote_signature = update_vote.make_certificate(&leader_secret_key.clone());
    SignedUpdateVote {
        vote: update_vote,
        signature: vote_signature,
    }
}
