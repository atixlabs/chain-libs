use crate::{
    fee::LinearFee,
    fragment::Fragment,
    testing::{
        builders::{
            create_initial_stake_pool_delegation, create_initial_stake_pool_registration,
            StakePoolBuilder,
        },
        data::{AddressDataValue, StakePool, Wallet},
        ledger::{ConfigBuilder, LedgerBuilder, TestLedger},
    },
};
use chain_addr::Discrimination;

use super::{
    template::{
        StakePoolDefBuilder, StakePoolTemplate, StakePoolTemplateBuilder, WalletTemplate,
        WalletTemplateBuilder,
    },
    Controller,
};

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ScenarioBuilderError {
    #[error("no config defined")]
    UndefinedConfig,
    #[error("no initials defined")]
    UndefinedInitials,
    #[error("stake pool '{alias}' must have at least one owner")]
    NoOwnersForStakePool { alias: String },
    #[error("with(...) method must be used for '{alias}' wallet in scenario builder. ")]
    UndefinedValueForWallet { alias: String },
}

pub struct ScenarioBuilder {
    config: ConfigBuilder,
    initials: Option<Vec<WalletTemplateBuilder>>,
    stake_pools: Option<Vec<StakePoolDefBuilder>>,
}

pub fn prepare_scenario() -> ScenarioBuilder {
    let default_config_builder = ConfigBuilder::new(0)
        .with_discrimination(Discrimination::Test)
        .with_fee(LinearFee::new(1, 1, 1));

    ScenarioBuilder {
        config: default_config_builder,
        initials: None,
        stake_pools: None,
    }
}

impl ScenarioBuilder {
    pub fn with_config(&mut self, config: ConfigBuilder) -> &mut Self {
        self.config = config;
        self
    }

    pub fn with_initials(&mut self, initials: Vec<&mut WalletTemplateBuilder>) -> &mut Self {
        self.initials = Some(initials.iter().map(|x| (**x).clone()).collect());
        self
    }

    pub fn with_stake_pools(&mut self, stake_pools: Vec<&mut StakePoolDefBuilder>) -> &mut Self {
        self.stake_pools = Some(stake_pools.iter().map(|x| (**x).clone()).collect());
        self
    }

    pub fn build(&self) -> Result<(TestLedger, Controller), ScenarioBuilderError> {
        if self.initials.is_none() {
            return Err(ScenarioBuilderError::UndefinedInitials);
        }

        let initials: Result<Vec<WalletTemplate>, ScenarioBuilderError> = self
            .initials
            .clone()
            .unwrap()
            .iter()
            .cloned()
            .map(|x| x.build())
            .collect();
        let initials: Vec<WalletTemplate> = initials?;
        let wallets: Vec<Wallet> = initials
            .iter()
            .cloned()
            .map(|x| self.build_wallet(x))
            .collect();
        let stake_pools_wallet_map = StakePoolTemplateBuilder::new(&initials);
        let stake_pool_templates: Vec<StakePoolTemplate> =
            stake_pools_wallet_map.build_stake_pool_templates(wallets.clone())?;
        let stake_pools = self.build_stake_pools(stake_pool_templates);
        let mut messages = self.build_stake_pools_fragments(&stake_pools, &wallets);
        messages.extend(self.build_delegation_fragments(&initials, &stake_pools, &wallets));
        let faucets: Vec<AddressDataValue> =
            wallets.iter().cloned().map(|x| x.as_account()).collect();
        let test_ledger = LedgerBuilder::from_config(self.config.clone())
            .faucets(&faucets)
            .certs(&messages)
            .build()
            .expect("cannot build test ledger");
        let block0_hash = test_ledger.block0_hash.clone();
        let fee = test_ledger.fee().clone();

        Ok((
            test_ledger,
            Controller::new(block0_hash, fee, wallets, stake_pools),
        ))
    }

    fn build_stake_pools_fragments(
        &self,
        stake_pools: &Vec<StakePool>,
        wallets: &Vec<Wallet>,
    ) -> Vec<Fragment> {
        stake_pools
            .iter()
            .cloned()
            .map(|stake_pool| {
                let owners_keys = stake_pool.info().owners;
                let owners: Vec<Wallet> = owners_keys
                    .iter()
                    .cloned()
                    .map(|pk| {
                        wallets
                            .iter()
                            .cloned()
                            .find(|x| x.public_key() == pk)
                            .expect("unknown key")
                    })
                    .collect();
                create_initial_stake_pool_registration(&stake_pool, &owners)
            })
            .collect()
    }

    fn build_delegation_fragments(
        &self,
        initials: &Vec<WalletTemplate>,
        stake_pools: &Vec<StakePool>,
        wallets: &Vec<Wallet>,
    ) -> Vec<Fragment> {
        initials
            .iter()
            .cloned()
            .filter(|x| x.delegates_stake_pool().is_some())
            .map(|wallet_template| {
                let stake_pool_alias = wallet_template.delegates_stake_pool().unwrap();
                let stake_pool = stake_pools
                    .iter()
                    .find(|sp| sp.alias() == stake_pool_alias)
                    .unwrap();
                let wallet_allias = wallet_template.alias();
                let wallet = wallets.iter().find(|w| w.alias() == wallet_allias).unwrap();
                create_initial_stake_pool_delegation(&stake_pool, &wallet)
            })
            .collect()
    }

    fn build_wallet(&self, template: WalletTemplate) -> Wallet {
        Wallet::new(&template.alias(), template.initial_value)
    }

    fn build_stake_pools(&self, stake_pool_templates: Vec<StakePoolTemplate>) -> Vec<StakePool> {
        stake_pool_templates
            .iter()
            .cloned()
            .map(|x| self.build_stake_pool(x))
            .collect()
    }

    fn build_stake_pool(&self, template: StakePoolTemplate) -> StakePool {
        let mut builder = StakePoolBuilder::new();
        builder.with_owners(template.owners());
        builder.with_alias(&template.alias());

        if let Some(stake_pools) = &self.stake_pools {
            let stake_pool_def_opt = stake_pools
                .iter()
                .cloned()
                .map(|x| x.build())
                .find(|x| x.alias == template.alias);

            if let Some(stake_pool_def) = stake_pool_def_opt {
                if let Some(pool_permission) = stake_pool_def.pool_permission() {
                    builder.with_pool_permissions(pool_permission);
                }
                if let Some(tax_type) = stake_pool_def.tax_type {
                    builder.with_tax_type(tax_type);
                }
                builder.with_reward_account(stake_pool_def.has_reward_account);
            }
        }
        builder.build()
    }
}

pub fn wallet(alias: &str) -> WalletTemplateBuilder {
    WalletTemplateBuilder::new(alias)
}

pub fn stake_pool(alias: &str) -> StakePoolDefBuilder {
    StakePoolDefBuilder::new(alias)
}
