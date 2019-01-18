/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.transporter

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */


import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.transporter.pods.TransporterServicesPod
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*
import javax.inject.Inject


@Component
class TransporterAtlas(
        private val hds: HikariDataSource,
        private var edm: EdmManager

) {

    companion object {
        private val logger = LoggerFactory.getLogger(TransporterAtlas::class.java)
        private val dateFormat = SimpleDateFormat("HH:mm:ss")
    }

    @Scheduled(fixedRate = 5000)
    fun sync() {
        logger.info("The time is now {}", dateFormat.format( Date()))
        logger.info(hds.toString())
    }
}