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
 */

package com.openlattice.transporter;

import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.conductor.codecs.pods.TypeCodecsPod;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.datastore.cassandra.CassandraTablesPod;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.transporter.pods.*;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTablesPod;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Transporter extends BaseRhizomeServer {
    static final Class<?>[] rhizomePods = new Class<?>[] { RegistryBasedHazelcastInstanceConfigurationPod.class };

    static final Class<?>[] transporterPods = new Class<?>[] {
            TransporterServicesPod.class,
            ByteBlobServicePod.class,
            TransporterSecurityPod.class,
            TypeCodecsPod.class,
            TransporterSparkPod.class,
            SharedStreamSerializersPod.class,
            PlasmaCoupling.class,
            Auth0Pod.class,
            CassandraTablesPod.class,
            MapstoresPod.class,
            JdbcPod.class,
            PostgresTablesPod.class,
            PostgresPod.class,
            Auth0Pod.class,
            AwsS3Pod.class
    };

    static {
        ObjectMappers.foreach( FullQualifiedNameJacksonSerializer::registerWithMapper );
    }

    public Transporter() {
        super( Pods.concatenate( RhizomeApplicationServer.DEFAULT_PODS, rhizomePods, transporterPods ) );
    }

    @Override
    public void start( String... activeProfiles ) throws Exception {
        super.start( activeProfiles );
    }

    public static void main( String[] args ) throws Exception {
        new Transporter().start( args );
    }
}
