//! Opt-in authenticated artifact checks composed with separate delivery authority.
//! Current trust checks are not atomic with a remote issuer or external effect.
use crate::{
    authenticated_artifacts::{AuthenticatedArtifacts, Clock, OperationClock, Trust},
    outbox::{
        ArtifactGuard, ConflictResolution, DeliveryAuthority, DeliveryRequest, DeliverySnapshot,
        Outbox, ResolutionAuthority, Transport,
    },
    tether_wire::{Peer, ResolutionBridge},
};
use rusqlite::Connection;
use std::path::Path;

struct Guard<'a, T> {
    trust: &'a T,
    clock: OperationClock<'a>,
}
impl<'a, T> Guard<'a, T> {
    fn new(trust: &'a T, clock: &'a dyn Clock) -> Self {
        Self {
            trust,
            clock: OperationClock::new(clock),
        }
    }
}
impl<T: Trust> ArtifactGuard for Guard<'_, T> {
    fn verify(&self, connection: &Connection, request: &DeliveryRequest) -> anyhow::Result<()> {
        anyhow::ensure!(request.valid(), "invalid authenticated delivery request");
        AuthenticatedArtifacts::read_retained(
            connection,
            &request.accepted,
            self.trust,
            &self.clock,
        )?;
        Ok(())
    }
}
/// Does not expose the legacy inner outbox. Legacy-only acceptance never suffices.
/// Trust/Clock and authority callbacks must be bounded and non-reentrant.
/// A post-effect denial leaves retained reservation/observations, never safe resend.
pub struct AuthenticatedOutbox {
    inner: Outbox,
}
impl AuthenticatedOutbox {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Outbox::open(path)?,
        })
    }
    pub fn deliver<A: DeliveryAuthority, P: Transport, T: Trust, C: Clock>(
        &mut self,
        request: &DeliveryRequest,
        authority: &A,
        transport: &mut P,
        trust: &T,
        clock: &C,
    ) -> anyhow::Result<DeliverySnapshot> {
        self.inner.deliver_guarded(
            request,
            authority,
            transport,
            Some(&Guard::new(trust, clock)),
        )
    }
    pub fn reconcile<A: DeliveryAuthority, P: Transport, T: Trust, C: Clock>(
        &mut self,
        request: &DeliveryRequest,
        authority: &A,
        transport: &mut P,
        trust: &T,
        clock: &C,
    ) -> anyhow::Result<DeliverySnapshot> {
        self.inner.reconcile_guarded(
            request,
            authority,
            transport,
            Some(&Guard::new(trust, clock)),
        )
    }
    pub fn resolve<
        A: DeliveryAuthority,
        R: ResolutionAuthority,
        P: Transport,
        T: Trust,
        C: Clock,
    >(
        &mut self,
        resolution: &ConflictResolution,
        authority: &A,
        resolver: &R,
        transport: &mut P,
        trust: &T,
        clock: &C,
    ) -> anyhow::Result<DeliverySnapshot> {
        self.inner.resolve_guarded(
            resolution,
            authority,
            resolver,
            transport,
            Some(&Guard::new(trust, clock)),
        )
    }
    /// Guards the first decision-retention peer call as well as later readback.
    pub fn resolve_wire<
        A: DeliveryAuthority,
        R: ResolutionAuthority,
        P: Peer,
        T: Trust,
        C: Clock,
    >(
        &mut self,
        bridge: &mut ResolutionBridge<P>,
        authority: &A,
        resolver: &R,
        trust: &T,
        clock: &C,
    ) -> anyhow::Result<DeliverySnapshot> {
        bridge.resolve_guarded(
            &mut self.inner,
            authority,
            resolver,
            Some(&Guard::new(trust, clock)),
        )
    }
}
