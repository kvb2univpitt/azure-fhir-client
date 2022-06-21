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
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;

/**
 *
 * Jun 19, 2022 9:41:20 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class PatientResourceClient extends AbstractResourceClient {

    public PatientResourceClient(IGenericClient client) {
        super(client);
    }

    public Bundle uploadPatients(List<Patient> patients) {
        List<Resource> resources = patients.stream()
                .map(e -> (Resource) e)
                .collect(Collectors.toList());

        return addResources(resources, "Patient");
    }

    public MethodOutcome deletePatient(Patient patient) {
        return client.delete().resource(patient).execute();
    }

    public Bundle deletePatients() {
        return deleteResources(getPatients());
    }

    public Patient getPatient(String id) {
        return client.read()
                .resource(Patient.class)
                .withId(id)
                .execute();
    }

    public Bundle getPatients() {
        return client
                .search()
                .forResource(Patient.class)
                .returnBundle(Bundle.class)
                .cacheControl(new CacheControlDirective().setNoCache(true))
                .execute();
    }

}
