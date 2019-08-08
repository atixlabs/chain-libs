use crate::{
    account::Identifier,
    certificate::{self, Certificate, CertificateContent},
    stake::StakePoolInfo,
    testing::address::AddressData,
    transaction::AccountIdentifier,
};

pub fn build_stake_delegation_cert(
    stake_pool: &StakePoolInfo,
    delegate_from: &AddressData,
) -> Certificate {
    let account_id =
        AccountIdentifier::from_single_account(Identifier::from(delegate_from.delegation_key()));

    let mut cert = Certificate {
        content: CertificateContent::StakeDelegation(certificate::StakeDelegation {
            stake_key_id: account_id,
            pool_id: stake_pool.to_id(),
        }),
        signatures: Vec::new(),
    };
    cert.sign(&delegate_from.private_key());
    cert
}

pub fn build_stake_pool_registration_cert(
    stake_pool: &StakePoolInfo,
    owner: &AddressData,
) -> Certificate {
    let mut cert = Certificate {
        content: CertificateContent::StakePoolRegistration(stake_pool.clone()),
        signatures: Vec::new(),
    };
    cert.sign(&owner.private_key());
    cert
}

pub fn build_stake_pool_retirement_cert(
    stake_pool: StakePoolInfo,
    owner: AddressData,
) -> Certificate {
    let mut cert = Certificate {
        content: CertificateContent::StakePoolRetirement(certificate::StakePoolRetirement {
            pool_id: stake_pool.to_id(),
            pool_info: stake_pool,
        }),
        signatures: Vec::new(),
    };
    cert.sign(&owner.private_key());
    cert
}
