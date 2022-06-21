/*
 * Copyright (C) 2022 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.azure.fhir.client;

import ca.uhn.fhir.rest.api.CacheControlDirective;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;

/**
 *
 * Jun 20, 2022 2:41:55 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class EncounterResourceClient extends AbstractResourceClient {

    private final IGenericClient client;

    public EncounterResourceClient(IGenericClient client) {
        this.client = client;
    }

    public MethodOutcome deleteEncounter(Encounter encounter) {
        return client.delete().resource(encounter).execute();
    }

    public Bundle deleteEncounters() {
        return deleteResources(getEncounters(), client);
    }

    public Bundle getEncounters() {
        return client
                .search()
                .forResource(Encounter.class)
                .returnBundle(Bundle.class)
                .cacheControl(new CacheControlDirective().setNoCache(true))
                .execute();
    }

}
