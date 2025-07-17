package cares.cam.ac.uk;

import java.util.List;

import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.sparqlbuilder.constraint.propertypath.builder.PropertyPathBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Prefix;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.json.JSONArray;
import org.json.JSONObject;

import com.cmclinnovations.stack.clients.blazegraph.BlazegraphClient;
import com.cmclinnovations.stack.clients.ontop.OntopClient;

import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;

public class QueryClient {
    RemoteStoreClient remoteStoreClient;
    String blazegraphUrl;
    String ontopUrl;

    static final Prefix PREFIX_DERIVATION = SparqlBuilder
            .prefix("derivation", Rdf.iri("https://www.theworldavatar.com/kg/ontoderivation/"));
    static final Prefix PREFIX_EXPOSURE = SparqlBuilder
            .prefix("exposure", Rdf.iri("https://www.theworldavatar.com/kg/ontoexposure/"));

    static final Iri IS_DERIVED_FROM = PREFIX_DERIVATION.iri("isDerivedFrom");
    static final Iri IS_DERIVED_USING = PREFIX_DERIVATION.iri("isDerivedUsing");
    static final Iri BELONGS_TO = PREFIX_DERIVATION.iri("belongsTo");
    static final Iri HAS_VALUE = PREFIX_EXPOSURE.iri("hasValue");
    static final Iri HAS_DISTANCE = PREFIX_EXPOSURE.iri("hasDistance");

    public QueryClient() {
        remoteStoreClient = new RemoteStoreClient();
        blazegraphUrl = BlazegraphClient.getInstance().readEndpointConfig().getUrl("kb");
        ontopUrl = OntopClient.getInstance("ontop").readEndpointConfig().getUrl();
    }

    JSONObject getResults(String iri) {
        SelectQuery query = Queries.SELECT();
        Iri subject = Rdf.iri(iri);
        Variable derivation = query.var();
        Variable exposure = query.var();
        Variable calculation = query.var();
        Variable exposureResult = query.var();
        Variable exposureValueVar = query.var();
        Variable distanceVar = query.var();
        Variable calculationType = query.var();

        TriplePattern gp1 = derivation.has(IS_DERIVED_FROM, subject)
                .andHas(PropertyPathBuilder.of(IS_DERIVED_FROM).then(RDFS.LABEL).build(), exposure)
                .andHas(IS_DERIVED_USING, calculation);
        TriplePattern gp2 = exposureResult.has(BELONGS_TO, derivation).andHas(HAS_VALUE, exposureValueVar);
        TriplePattern gp3 = calculation.isA(calculationType).andHas(HAS_DISTANCE, distanceVar);

        query.where(gp1, gp2, gp3).select(exposureValueVar, exposure, calculationType, distanceVar).prefix(
                PREFIX_DERIVATION, PREFIX_EXPOSURE);

        JSONArray queryResult = remoteStoreClient.executeFederatedQuery(List.of(blazegraphUrl, ontopUrl),
                query.getQueryString());

        JSONObject metadata = new JSONObject();

        for (int i = 0; i < queryResult.length(); i++) {
            String datasetName = queryResult.getJSONObject(i).getString(exposure.getVarName());
            String calculationName = queryResult.getJSONObject(i).getString(calculationType.getVarName());
            calculationName = calculationName.substring(calculationName.lastIndexOf('/') + 1);
            double distance = parseRdfLiteral(queryResult.getJSONObject(i).getString(distanceVar.getVarName()));
            double exposureValue = parseRdfLiteral(
                    queryResult.getJSONObject(i).getString(exposureValueVar.getVarName()));

            String distanceKey = distance + " m";

            if (!metadata.has(datasetName)) {
                JSONObject exposureJson = new JSONObject();
                JSONObject calculationJson = new JSONObject();

                metadata.put(datasetName, exposureJson);
                exposureJson.put(calculationName, calculationJson);
                calculationJson.put(distanceKey, exposureValue);
            } else {
                if (metadata.getJSONObject(datasetName).has(calculationName)) {
                    metadata.getJSONObject(datasetName).getJSONObject(calculationName).put(distanceKey, exposureValue);
                } else {
                    JSONObject calculationJson = new JSONObject();
                    calculationJson.put(distanceKey, exposureValue);
                    metadata.getJSONObject(datasetName).put(calculationName, calculationJson);
                }
            }

        }

        return metadata;
    }

    /**
     * parse something like "1000"^^<http://www.w3.org/2001/XMLSchema#integer>
     */
    double parseRdfLiteral(String literal) {
        try {
            return Double.parseDouble(literal);
        } catch (NumberFormatException e) {
            int start = literal.indexOf('"') + 1;
            int end = literal.indexOf('"', start);

            String value = literal.substring(start, end);
            return Double.parseDouble(value);
        }
    }
}
