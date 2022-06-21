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

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Resource;

/**
 *
 * Jun 20, 2022 11:41:37 AM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public abstract class AbstractResourceClient {
    
    protected MethodOutcome addResource(Resource resource, IGenericClient client) {        
       return client.create().resource(resource).execute();
    }
    
    public MethodOutcome deleteResource(Resource resource, IGenericClient client) {        
        return client.delete().resource(resource).execute();
    }

    protected Bundle addResources(List<Resource> resources, String url, IGenericClient client) {
        Bundle bundle = new Bundle();
        bundle.setType(Bundle.BundleType.TRANSACTION);

        resources.forEach(resource -> bundle.addEntry()
                .setResource(resource)
                .getRequest()
                .setUrl(url)
                .setMethod(Bundle.HTTPVerb.POST));

        return client.transaction().withBundle(bundle).execute();
    }

    protected Bundle deleteResources(Bundle searchBundle, IGenericClient client) {
        Bundle deleteBundle = new Bundle();
        deleteBundle.setType(Bundle.BundleType.TRANSACTION);

        searchBundle.getEntry()
                .forEach(e -> deleteBundle
                .addEntry()
                .getRequest().setUrl(e.getFullUrl())
                .setMethod(Bundle.HTTPVerb.DELETE));

        while (searchBundle.getLink(IBaseBundle.LINK_NEXT) != null) {
            searchBundle = client
                    .loadPage()
                    .next(searchBundle)
                    .execute();

            searchBundle.getEntry().stream()
                    .forEach(e -> deleteBundle
                    .addEntry()
                    .getRequest().setUrl(e.getFullUrl())
                    .setMethod(Bundle.HTTPVerb.DELETE));
        }

        return client.transaction().withBundle(deleteBundle).execute();
    }

}
