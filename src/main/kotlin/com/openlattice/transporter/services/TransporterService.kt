package com.openlattice.transporter.services

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.events.ClearAllDataEvent
import com.openlattice.edm.events.EntityTypeCreatedEvent
import com.openlattice.edm.events.EntityTypeDeletedEvent
import com.openlattice.edm.set.EntitySetFlag
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
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
        private val dataModelService: EdmManager,
        private val partitionManager: PartitionManager
)
{
    companion object {
        val logger = LoggerFactory.getLogger(TransporterService::class.java)
    }

    private val transporterDataSource = AssemblerConnectionManager.createDataSource( "transporter", configuration.server, configuration.ssl )
    private val entityTypes: MutableMap<UUID, TransporterEntityTypeManager> = ConcurrentHashMap()


    init {
        dataModelService.entityTypes.map { et -> et.id to TransporterEntityTypeManager(et, dataModelService, partitionManager) }.toMap(entityTypes)
        logger.info("Creating {} entity set tables", entityTypes.size)
        transporterDataSource.connection.use { c ->
            entityTypes.values.forEach {
                it.createTables(c)
            }
        }
        // TODO: ensure FDW is in place
        logger.info("Entity set tables created")

        if (configuration.once) {
            try {
                pollOnce()
            } catch (e: Exception) {
                logger.error("error", e)
                throw e
            } finally {
                exitProcess(0)
            }
        } else {
            eventBus.register(this)
            executor.submit {
                pollInfinitely()
            }
        }
    }

    private fun pollOnce() {
        entitySetService.getEntitySets().filterNot { it.flags.contains(EntitySetFlag.AUDIT) }.forEach { es ->
//        entitySetService.getEntitySet(UUID.fromString("cd92c07f-175a-4f2c-a6d4-8e8ba2c1d6ee"))?.let { es ->
            entityTypes[es.entityTypeId]?.updateEntitySet(transporterDataSource, es)
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

    @Subscribe
    fun handleClearAllData(e: ClearAllDataEvent) {
        // TODO: truncate all tables
    }
}

