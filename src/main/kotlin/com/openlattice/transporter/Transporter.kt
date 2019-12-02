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

import com.dataloom.mappers.ObjectMappers
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

fun main(args: Array<String>) {
    ObjectMappers.foreach(FullQualifiedNameJacksonSerializer::registerWithMapper)
    System.out.println("Transporter is live !")
}
//
//private val datastorePods = arrayOf(
//        ByteBlobServicePod::class.java, DatastoreServicesPod::class.java, TypeCodecsPod::class.java,
//        SharedStreamSerializersPod::class.java, AwsS3Pod::class.java, JdbcPod::class.java,
//        DatastoreNeuronPod::class.java, PostgresPod::class.java, AuditingConfigurationPod::class.java
//)
//private val rhizomePods = arrayOf(RegistryBasedHazelcastInstanceConfigurationPod::class.java, Auth0Pod::class.java)
//private val webPods = arrayOf<Class<*>>(DatastoreServletsPod::class.java, DatastoreSecurityPod::class.java)
//
//
//
//class Transporter : BaseRhizomeServer(
//        RhizomeUtils.Pods.concatenate(
//                pods,
//                webPods,
//                rhizomePods,
//                RhizomeApplicationServer.DEFAULT_PODS,
//                datastorePods
//        )) {
//
//}
