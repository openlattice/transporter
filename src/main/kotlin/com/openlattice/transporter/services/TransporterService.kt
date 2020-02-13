package com.openlattice.transporter.services

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.data.storage.buildPreparableFiltersSql
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.events.EntityTypeCreatedEvent
import com.openlattice.edm.events.EntityTypeDeletedEvent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

@Service
final class TransporterService(
        private val executor: ListeningExecutorService,
        private val eventBus: EventBus,
        private val configuration: TransporterConfiguration,
        private val hazelcastInstance: HazelcastInstance,
        private val entitySetService: EntitySetManager,
        private val dataModelService: EdmManager
)
{
    companion object {
        val logger = LoggerFactory.getLogger(TransporterService::class.java)
    }

    private val transporterDataSource = AssemblerConnectionManager.createDataSource( "transporter", configuration.server, configuration.ssl )
    private val entityTypes: MutableMap<UUID, TransporterEntityTypeState> = ConcurrentHashMap()


    init {
        dataModelService.entityTypes.map { et -> et.id to TransporterEntityTypeState(et, dataModelService) }.toMap(entityTypes)
        logger.info("Creating {} entity set tables", entityTypes.size)
        transporterDataSource.connection.use { c ->
            entityTypes.values.forEach {
                it.createTables(c)
            }
        }
        // TODO: ensure FDW is in place
        logger.info("Entity set tables created")

        if (configuration.once) {
            pollOnce()
            exitProcess(0)
        } else {
            eventBus.register(this)
            executor.submit {
                pollInfinitely()
            }
        }
    }

    private fun pollOnce() {
        transporterDataSource.connection.use {
            entitySetService.getEntitySets().forEach {es ->
//            entitySetService.getEntitySet(UUID.fromString("3a1c9bb1-5e72-4735-92b7-715cc9555eb2"))?.let { es ->
                entityTypes[es.entityTypeId]?.updateEntitySet(it, es)
            }
        }
    }

    private fun pollInfinitely() {
        Thread.currentThread().name = "transporter-change-poller"
        while (true) {
            try {
                pollOnce()
                Thread.sleep(TimeUnit.SECONDS.toMillis(1))
            } catch (t: Throwable) {
                logger.error("Error in poller, terminating", t)
                exitProcess(1)
            }
        }
    }

    @Subscribe
    fun handleEntityTypeCreated(e: EntityTypeCreatedEvent) {
        // TODO: create new entity type entry

    }

    @Subscribe
    fun handleEntityTypeDeleted(e: EntityTypeDeletedEvent) {
        // TODO: remove entity type entries
    }


    private fun runLinkedQuery(c: Connection, typeId: UUID, propertiesToUpdate: MutableSet<UUID>) {
        val properties = propertiesToUpdate.associateWith( dataModelService::getPropertyType )
        val (sql, _) = buildPreparableFiltersSql(
                1,
                properties,
                mapOf(),
                setOf(),
                linking = true,
                idsPresent = false,
                detailed = false,
                partitionsPresent = false
        )
        logger.info("{}", sql)
    }

    private fun runUnlinkedQuery(c: Connection, typeId: UUID, propertiesToUpdate: Set<UUID>) {
        val properties = propertiesToUpdate.associateWith( dataModelService::getPropertyType )
        val (sql, _) = buildPreparableFiltersSql(
                1,
                properties,
                mapOf(),
                setOf(),
                linking = false,
                idsPresent = false,
                detailed = false,
                partitionsPresent = false
        )
        logger.info("{}", sql)
    }
}

