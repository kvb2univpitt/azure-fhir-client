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

import ca.uhn.fhir.rest.client.api.IGenericClient;
import edu.pitt.dbmi.fhir.resource.mapper.r4.brainai.EncounterResourceMapper;
import edu.pitt.dbmi.fhir.resource.mapper.r4.brainai.ObservationResourceMapper;
import edu.pitt.dbmi.fhir.resource.mapper.r4.brainai.PatientResourceMapper;
import edu.pitt.dbmi.fhir.resource.mapper.util.Delimiters;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;

/**
 *
 * Jun 21, 2022 10:40:54 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class BrainAiResourceClient extends AbstractResourceClient {

    public BrainAiResourceClient(IGenericClient client) {
        super(client);
    }
    
    public void addResources(Path resourceDirectory) throws IOException {
        List<Patient> patients = getPatients(Paths.get(resourceDirectory.toString(), "patients.tsv"));
    }

    public List<Observation> getObservations(Path bundleFile) throws IOException {
        return ObservationResourceMapper.getObservations(bundleFile, Delimiters.TAB_DELIM);
    }

    public List<Encounter> getEncounters(Path tsvFile) throws IOException {
        return EncounterResourceMapper.getEncounters(tsvFile, Delimiters.TAB_DELIM);
    }

    public List<Patient> getPatients(Path tsvFile) throws IOException {
        return PatientResourceMapper.getPatients(tsvFile, Delimiters.TAB_DELIM);
    }

}
