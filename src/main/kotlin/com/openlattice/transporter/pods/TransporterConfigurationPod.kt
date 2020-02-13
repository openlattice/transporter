package com.openlattice.transporter.pods

import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.transporter.services.TransporterConfiguration
import org.springframework.context.annotation.Bean
import javax.inject.Inject

class TransporterConfigurationPod {
    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Bean
    fun transporterConfiguration(): TransporterConfiguration {
        return configurationLoader.logAndLoad("transporter", TransporterConfiguration::class.java)
    }
}