package com.openlattice.transporter.pods

import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.edm.PostgresEdmManager
import javax.inject.Inject

class EntitySetManagerPod {
    @Inject
    private lateinit var aclKeyReservationService: HazelcastAclKeyReservationService

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var pgEdmManager: PostgresEdmManager

}