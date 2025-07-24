package cares.cam.ac.uk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
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
import com.cmclinnovations.stack.clients.postgis.PostGISClient;

import uk.ac.cam.cares.jps.base.query.RemoteRDBStoreClient;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesClient;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesClientFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryClient {
    private static final Logger LOGGER = LogManager.getLogger(QueryClient.class);

    RemoteStoreClient remoteStoreClient;
    RemoteStoreClient ontopClient;
    RemoteRDBStoreClient remoteRDBStoreClient;
    String blazegraphUrl;
    String ontopUrl;

    static final Prefix PREFIX_DERIVATION = SparqlBuilder
            .prefix("derivation", Rdf.iri("https://www.theworldavatar.com/kg/ontoderivation/"));
    static final Prefix PREFIX_EXPOSURE = SparqlBuilder
            .prefix("exposure", Rdf.iri("https://www.theworldavatar.com/kg/ontoexposure/"));
    static final Prefix PREFIX_TIMESERIES = SparqlBuilder
            .prefix("timeseries", Rdf.iri("https://www.theworldavatar.com/kg/ontotimeseries/"));

    static final Iri IS_DERIVED_FROM = PREFIX_DERIVATION.iri("isDerivedFrom");
    static final Iri IS_DERIVED_USING = PREFIX_DERIVATION.iri("isDerivedUsing");
    static final Iri BELONGS_TO = PREFIX_DERIVATION.iri("belongsTo");
    static final Iri HAS_VALUE = PREFIX_EXPOSURE.iri("hasValue");
    static final Iri HAS_DISTANCE = PREFIX_EXPOSURE.iri("hasDistance");
    static final Iri HAS_TIME_SERIES = PREFIX_TIMESERIES.iri("hasTimeSeries");
    static final Iri HAS_TIME_CLASS = PREFIX_TIMESERIES.iri("hasTimeClass");

    public QueryClient() {
        blazegraphUrl = BlazegraphClient.getInstance().readEndpointConfig().getUrl("kb");
        ontopUrl = OntopClient.getInstance("ontop").readEndpointConfig().getUrl();

        remoteStoreClient = BlazegraphClient.getInstance().getRemoteStoreClient("kb");
        ontopClient = new RemoteStoreClient(ontopUrl);

        String rdbUrl = PostGISClient.getInstance().readEndpointConfig().getJdbcURL("postgres");
        String user = PostGISClient.getInstance().readEndpointConfig().getUsername();
        String password = PostGISClient.getInstance().readEndpointConfig().getPassword();

        remoteRDBStoreClient = new RemoteRDBStoreClient(rdbUrl, user, password);
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

            String formattedValue = String.format("%.0f", exposureValue);

            String distanceKey = String.format("%.0f", distance) + " m";

            if (!metadata.has(datasetName)) {
                JSONObject exposureJson = new JSONObject();
                JSONObject calculationJson = new JSONObject();
                calculationJson.put("collapse", true);

                metadata.put(datasetName, exposureJson);
                exposureJson.put(calculationName, calculationJson);
                calculationJson.put(distanceKey, formattedValue);
            } else {
                if (metadata.getJSONObject(datasetName).has(calculationName)) {
                    metadata.getJSONObject(datasetName).getJSONObject(calculationName).put(distanceKey, formattedValue);
                } else {
                    JSONObject calculationJson = new JSONObject();
                    calculationJson.put("collapse", true);
                    calculationJson.put(distanceKey, formattedValue);
                    metadata.getJSONObject(datasetName).put(calculationName, calculationJson);
                }
            }

        }

        return metadata;
    }

    JSONObject getResultsTrajectory(String iri, String lowerbound, String upperbound) {
        SelectQuery query = Queries.SELECT();
        Iri subject = Rdf.iri(iri);
        Variable derivation = query.var();
        Variable exposure = query.var();
        Variable calculation = query.var();
        Variable exposureResultVar = query.var();
        Variable distanceVar = query.var();
        Variable calculationType = query.var();
        Variable calculationLabel = query.var();

        TriplePattern gp1 = derivation.has(IS_DERIVED_FROM, subject)
                .andHas(PropertyPathBuilder.of(IS_DERIVED_FROM).then(RDFS.LABEL).build(), exposure)
                .andHas(IS_DERIVED_USING, calculation);
        TriplePattern gp2 = exposureResultVar.has(BELONGS_TO, derivation);
        TriplePattern gp3 = calculation.isA(calculationType).andHas(HAS_DISTANCE, distanceVar);
        TriplePattern gp4 = calculationType.has(RDFS.LABEL, calculationLabel);

        query.where(gp1, gp2, gp3, gp4.optional())
                .select(exposure, calculationType, distanceVar, exposureResultVar, calculationLabel)
                .prefix(PREFIX_DERIVATION, PREFIX_EXPOSURE);

        JSONArray queryResult = remoteStoreClient.executeFederatedQuery(List.of(blazegraphUrl, ontopUrl),
                query.getQueryString());

        JSONObject metadata = new JSONObject();

        TimeSeriesClient timeSeriesClient;
        try {
            timeSeriesClient = TimeSeriesClientFactory.getInstance(remoteStoreClient, List.of(iri));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = remoteRDBStoreClient.getConnection()) {
            for (int i = 0; i < queryResult.length(); i++) {
                String datasetName = queryResult.getJSONObject(i).getString(exposure.getVarName());

                String calculationName;

                if (queryResult.getJSONObject(i).has(calculationLabel.getVarName())) {
                    calculationName = queryResult.getJSONObject(i).getString(calculationLabel.getVarName());
                } else {
                    calculationName = queryResult.getJSONObject(i).getString(calculationType.getVarName());
                    calculationName = calculationName.substring(calculationName.lastIndexOf('/') + 1);
                }

                double distance = parseRdfLiteral(queryResult.getJSONObject(i).getString(distanceVar.getVarName()));
                String distanceKey = String.format("%.0f", distance) + " m";

                String exposureResult = queryResult.getJSONObject(i).getString(exposureResultVar.getVarName());

                double exposureValue = getExposureResult(timeSeriesClient, exposureResult, lowerbound, upperbound,
                        conn);
                String formattedValue = String.format("%.0f", exposureValue);

                if (!metadata.has(datasetName)) {
                    JSONObject exposureJson = new JSONObject();
                    JSONObject calculationJson = new JSONObject();
                    calculationJson.put("collapse", true);

                    metadata.put(datasetName, exposureJson);
                    exposureJson.put(calculationName, calculationJson);
                    calculationJson.put(distanceKey, formattedValue);
                } else {
                    if (metadata.getJSONObject(datasetName).has(calculationName)) {
                        metadata.getJSONObject(datasetName).getJSONObject(calculationName).put(distanceKey,
                                formattedValue);
                    } else {
                        JSONObject calculationJson = new JSONObject();
                        calculationJson.put("collapse", true);
                        calculationJson.put(distanceKey, formattedValue);
                        metadata.getJSONObject(datasetName).put(calculationName, calculationJson);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return metadata;
    }

    double getExposureResult(TimeSeriesClient timeSeriesClient, String iri, Object lowerbound, Object upperbound,
            Connection conn) {
        TimeSeries timeSeries = timeSeriesClient.getTimeSeriesWithinBounds(List.of(iri), lowerbound, upperbound, conn);
        Set<Double> resultSet = new HashSet<>(timeSeries.getValuesAsDouble(iri));
        if (resultSet.size() > 1) {
            throw new RuntimeException("Results related to trips to be implemented");
        }
        return resultSet.iterator().next();
    }

    /**
     * parse something like "1000"^^<http://www.w3.org/2001/XMLSchema#integer>
     */
    private double parseRdfLiteral(String literal) {
        try {
            return Double.parseDouble(literal);
        } catch (NumberFormatException e) {
            int start = literal.indexOf('"') + 1;
            int end = literal.indexOf('"', start);

            String value = literal.substring(start, end);
            return Double.parseDouble(value);
        }
    }

    private Object convertTimeForTimeSeries(String time) {
        try {
            if (time == null) {
                return null;
            } else {
                return Long.parseLong(time);
            }
        } catch (NumberFormatException e) {
            String errmsg = "Only epoch seconds supported for now";
            throw new RuntimeException(errmsg, e);
        }

    }

    private String getJavaTimeClass(String iri) {
        SelectQuery query = Queries.SELECT();
        Variable classNameVar = query.var();
        query.where(
                Rdf.iri(iri).has(PropertyPathBuilder.of(HAS_TIME_SERIES).then(HAS_TIME_CLASS).build(), classNameVar))
                .prefix(PREFIX_TIMESERIES);

        JSONArray queryResult = remoteStoreClient.executeQuery(query.getQueryString());
        return queryResult.getJSONObject(0).getString(classNameVar.getVarName());
    }
}
