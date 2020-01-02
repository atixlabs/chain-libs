use super::genesis::GenesisPraosLeader;
use chain_crypto::{
    testing::TestCryptoGen, Curve25519_2HashDH, PublicKey, SecretKey, SumEd25519_12,
};
use lazy_static::lazy_static;
use quickcheck::{Arbitrary, Gen};
use rand_core;

impl Arbitrary for GenesisPraosLeader {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        use rand_core::SeedableRng;
        lazy_static! {
            static ref PK_KES: PublicKey<SumEd25519_12> = {
                let sk: SecretKey<SumEd25519_12> =
                    SecretKey::generate(&mut rand_chacha::ChaChaRng::from_seed([0; 32]));
                sk.to_public()
            };
        }

        let tcg = TestCryptoGen::arbitrary(g);
        let mut rng = tcg.get_rng(0);
        let vrf_sk: SecretKey<Curve25519_2HashDH> = SecretKey::generate(&mut rng);
        GenesisPraosLeader {
            vrf_public_key: vrf_sk.to_public(),
            kes_public_key: PK_KES.clone(),
        }
    }
}