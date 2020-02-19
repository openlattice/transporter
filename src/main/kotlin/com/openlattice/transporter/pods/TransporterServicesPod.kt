package com.openlattice.transporter.pods

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizationQueryService
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.SchemaQueryService
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService
import com.openlattice.transporter.services.TransporterConfiguration
import com.openlattice.transporter.services.TransporterService
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import javax.inject.Inject

/**
 * @author David Terrell &lt;dbt@openlattice.com&gt;
 */
@Import(TransporterConfigurationPod::class, AuditingConfigurationPod::class)
class TransporterServicesPod {
    @Inject
    private lateinit var executor: ListeningExecutorService

    @Inject
    private lateinit var eventBus: EventBus
    @Inject
    private lateinit var configuration: TransporterConfiguration
    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance
    @Inject
    private lateinit var hikariDataSource: HikariDataSource
    @Inject
    private lateinit var auditingConfiguration: AuditingConfiguration


    @Bean
    fun aclKeyReservationService(): HazelcastAclKeyReservationService {
        return HazelcastAclKeyReservationService(hazelcastInstance)
    }

    @Bean
    fun authorizationQueryService(): AuthorizationQueryService {
        return AuthorizationQueryService(hikariDataSource, hazelcastInstance)
    }

    @Bean
    fun authorizationManager(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcastInstance, authorizationQueryService(), eventBus)
    }


    @Bean
    fun edmManager(): PostgresEdmManager {
        return PostgresEdmManager( hikariDataSource, hazelcastInstance );
    }

    @Bean
    fun entityTypeManager(): PostgresTypeManager {
        return PostgresTypeManager(hikariDataSource)
    }

    @Bean
    fun schemaQueryService(): SchemaQueryService {
        return PostgresSchemaQueryService(hikariDataSource)
    }

    @Bean
    fun schemaManager(): HazelcastSchemaManager {
        return HazelcastSchemaManager(hazelcastInstance, schemaQueryService())
    }


    @Bean
    fun dataModelService(): EdmManager {
        return EdmService(
                hikariDataSource,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                edmManager(),
                entityTypeManager(),
                schemaManager()
        )
    }

    @Bean
    fun partitionManager(): PartitionManager {
        return PartitionManager(hazelcastInstance, hikariDataSource)
    }

    @Bean
    fun entitySetManager(): EntitySetManager {
        return EntitySetService(
                hazelcastInstance,
                eventBus,
                edmManager(),
                aclKeyReservationService(),
                authorizationManager(),
                partitionManager(),
                dataModelService(),
                auditingConfiguration
        )
    }

    @Bean
    fun transporterService(): TransporterService {
        return TransporterService(executor, eventBus, configuration, hazelcastInstance, hikariDataSource, dataModelService(), partitionManager(), entitySetManager())
    }
}