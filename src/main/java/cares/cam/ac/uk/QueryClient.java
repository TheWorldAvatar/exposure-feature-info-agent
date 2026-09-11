package cares.cam.ac.uk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.sparqlbuilder.constraint.Expressions;
import org.eclipse.rdf4j.sparqlbuilder.core.Prefix;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.json.JSONArray;
import org.json.JSONObject;

import com.cmclinnovations.stack.clients.blazegraph.BlazegraphClient;
import com.cmclinnovations.stack.clients.ontop.OntopClient;
import com.cmclinnovations.stack.clients.postgis.PostGISClient;
import com.cmclinnovations.stack.clients.rdf4j.Rdf4jClient;

import cares.cam.ac.uk.classes.CalculationMethod;
import cares.cam.ac.uk.classes.ExposureResult;
import uk.ac.cam.cares.jps.base.derivation.ValuesPattern;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;
import uk.ac.cam.cares.jps.base.query.RemoteRDBStoreClient;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;

public class QueryClient {
    private static final Logger LOGGER = LogManager.getLogger(QueryClient.class);

    private static String formatCalculationLabel(String name) {
        String label = name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2")
                .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
                .toLowerCase(Locale.ROOT);
        return label.isEmpty() ? label : Character.toUpperCase(label.charAt(0)) + label.substring(1);
    }

    RemoteStoreClient federateClient;
    RemoteStoreClient ontopClient;
    RemoteStoreClient blazegraphClient;

    static final Prefix PREFIX_DERIVATION = SparqlBuilder
            .prefix("derivation", Rdf.iri("https://www.theworldavatar.com/kg/ontoderivation/"));
    static final Prefix PREFIX_EXPOSURE = SparqlBuilder
            .prefix("exposure", Rdf.iri("https://www.theworldavatar.com/kg/ontoexposure/"));
    static final Prefix PREFIX_TIMESERIES = SparqlBuilder
            .prefix("timeseries", Rdf.iri("https://www.theworldavatar.com/kg/ontotimeseries/"));

    static final Iri IS_DERIVED_FROM = PREFIX_DERIVATION.iri("isDerivedFrom");
    static final Iri HAS_CALCULATION_METHOD = PREFIX_EXPOSURE.iri("hasCalculationMethod");
    static final Iri BELONGS_TO = PREFIX_DERIVATION.iri("belongsTo");
    static final Iri HAS_VALUE = PREFIX_EXPOSURE.iri("hasValue");
    static final Iri HAS_DISTANCE = PREFIX_EXPOSURE.iri("hasDistance");
    static final Iri HAS_TIME_SERIES = PREFIX_TIMESERIES.iri("hasTimeSeries");
    static final Iri HAS_TIME_CLASS = PREFIX_TIMESERIES.iri("hasTimeClass");
    static final Iri HAS_UNIT = PREFIX_EXPOSURE.iri("hasUnit");
    static final Iri HAS_DATASET_FILTER = PREFIX_EXPOSURE.iri("hasDatasetFilter");
    static final Iri HAS_FILTER_COLUMN = PREFIX_EXPOSURE.iri("hasFilterColumn");
    static final Iri HAS_FILTER_VALUE = PREFIX_EXPOSURE.iri("hasFilterValue");

    static final Iri TRIP = PREFIX_EXPOSURE.iri("Trip");

    public QueryClient() {
        String stackOutgoing = Rdf4jClient.getInstance().readEndpointConfig().getOutgoingRepositoryUrl();
        federateClient = new RemoteStoreClient(stackOutgoing);

        String ontopUrl = OntopClient.getInstance("ontop").readEndpointConfig().getUrl();
        ontopClient = new RemoteStoreClient(ontopUrl);

        String namespace;
        if (Config.NAMESPACE != null) {
            namespace = Config.NAMESPACE;
            blazegraphClient = BlazegraphClient.getInstance().getRemoteStoreClient(namespace);
        }
    }

    JSONObject getExposureResults(String iri) {
        SelectQuery query = Queries.SELECT();
        Iri subject = Rdf.iri(iri);
        Variable derivation = query.var();
        Variable exposure = query.var();
        Variable calculation = query.var();
        Variable exposureResult = query.var();
        Variable exposureValueVar = query.var();
        Variable unitVar = query.var();

        GraphPattern gp1 = derivation.has(IS_DERIVED_FROM, subject).andHas(IS_DERIVED_FROM, exposure)
                .filter(Expressions.notEquals(exposure, Rdf.iri(iri)));
        TriplePattern gp2 = exposureResult.has(BELONGS_TO, derivation).andHas(HAS_VALUE, exposureValueVar)
                .andHas(HAS_CALCULATION_METHOD, calculation).andHas(HAS_UNIT, unitVar);

        query.where(gp1, gp2).select(exposureValueVar, exposure, calculation, unitVar).prefix(
                PREFIX_DERIVATION, PREFIX_EXPOSURE).distinct();

        JSONArray queryResult = ontopClient.executeQuery(query.getQueryString());

        Map<String, CalculationMethod> calculationMap = new HashMap<>();
        Set<String> exposureSet = new HashSet<>();
        List<ExposureResult> resultList = new ArrayList<>();

        for (int i = 0; i < queryResult.length(); i++) {
            String exposureIri = queryResult.getJSONObject(i).getString(exposure.getVarName());
            String calculationIri = queryResult.getJSONObject(i).getString(calculation.getVarName());
            double value = queryResult.getJSONObject(i).getDouble(exposureValueVar.getVarName());
            String unit = queryResult.getJSONObject(i).getString(unitVar.getVarName());

            calculationMap.computeIfAbsent(calculationIri, k -> new CalculationMethod(k));
            exposureSet.add(exposureIri);
            resultList.add(new ExposureResult(exposureIri, calculationIri, value, unit));
        }
        return formatExposureResults(resultList, calculationMap, exposureSet);
    }

    JSONObject getExposureResultsSql(String iri) {
        RemoteRDBStoreClient rdbClient = PostGISClient.getInstance().getRemoteStoreClient(Config.DATABASE);
        String sql = "SELECT DISTINCT exposure, calculation, value, unit, percentile "
                + "FROM exposure_result WHERE subject = ? "
                + "AND exposure <> subject AND calculation IS NOT NULL "
                + "AND value IS NOT NULL AND unit IS NOT NULL";
        Map<String, CalculationMethod> calculationMap = new HashMap<>();
        Set<String> exposureSet = new HashSet<>();
        List<ExposureResult> resultList = new ArrayList<>();
        try (Connection connection = rdbClient.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, iri);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String exposureIri = rows.getString("exposure");
                    String calculationIri = rows.getString("calculation");
                    double percentileValue = rows.getDouble("percentile");
                    Double percentile = rows.wasNull() ? null : percentileValue;
                    calculationMap.computeIfAbsent(calculationIri, CalculationMethod::new);
                    exposureSet.add(exposureIri);
                    resultList.add(new ExposureResult(exposureIri, calculationIri,
                            rows.getDouble("value"), rows.getString("unit"), percentile));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query exposure results", e);
        }
        return formatExposureResults(resultList, calculationMap, exposureSet);
    }

    private JSONObject formatExposureResults(List<ExposureResult> resultList,
            Map<String, CalculationMethod> calculationMap, Set<String> exposureSet) {
        setCalculationProperties(calculationMap);
        Map<String, String> exposureMap = getExposureName(exposureSet);

        JSONObject metadata = new JSONObject();

        for (ExposureResult result : resultList) {
            String datasetName;
            if (exposureMap.containsKey(result.getExposureIri())) {
                datasetName = exposureMap.get(result.getExposureIri());
            } else {
                datasetName = result.getExposureIri();
            }

            if (!calculationMap.containsKey(result.getCalculationIri())) {
                continue;
            }

            CalculationMethod calcMethod = calculationMap.get(result.getCalculationIri());
            if (!metadata.has(datasetName)) {
                JSONObject datasetJson = new JSONObject();
                metadata.put(datasetName, datasetJson);
            }

            JSONObject datasetJson = metadata.getJSONObject(datasetName);
            datasetJson.put("collapse", true);

            // cycle through to check each dataset filter is initialised
            JSONObject currentLevel = datasetJson;
            for (String filter : calcMethod.getDatasetFilters()) {
                if (!currentLevel.has(filter)) {
                    JSONObject filterJson = new JSONObject();
                    filterJson.put("collapse", true);
                    currentLevel.put(filter, filterJson);
                }

                // sort year in ascending order
                if (filter.contains("year")) {
                    if (currentLevel.has("display_order")) {
                        List<String> order = new ArrayList<>(
                                currentLevel.getJSONArray("display_order").toList().stream()
                                        .map(Object::toString).toList());

                        if (!order.contains(filter)) {
                            order.add(filter);
                            List<String> yearSorted = order.stream().filter(d -> !d.contentEquals("collapse"))
                                    .sorted(Comparator.comparingInt(
                                            s -> Integer.parseInt(s.split("=")[1])))
                                    .collect(Collectors.toList());
                            currentLevel.put("display_order", yearSorted);
                        }
                    } else {
                        currentLevel.put("display_order", List.of(filter));
                    }
                }

                currentLevel = currentLevel.getJSONObject(filter);
            }

            // now actually adding values
            currentLevel = datasetJson;
            if (!calcMethod.getDatasetFilters().isEmpty()) {
                for (String filter : calcMethod.getDatasetFilters()) {
                    currentLevel = currentLevel.getJSONObject(filter);
                }
            }

            if (!currentLevel.has(calcMethod.getName())) {
                JSONObject newJson = new JSONObject();
                newJson.put("collapse", true);
                currentLevel.put(calcMethod.getName(), newJson);
            }

            currentLevel = currentLevel.getJSONObject(calcMethod.getName());

            String formattedDistance = calcMethod.getFormattedDistance();
            if (formattedDistance != null) {
                currentLevel.put(formattedDistance, result.getFormattedValue());

                // the purpose of this is to display distances in ascending order
                if (currentLevel.has("display_order")) {
                    List<String> order = new ArrayList<>(currentLevel.getJSONArray("display_order").toList().stream()
                            .map(Object::toString).toList());

                    if (!order.contains(formattedDistance)) {
                        order.add(formattedDistance);
                        List<String> distanceSorted = order.stream().filter(d -> !d.contentEquals("collapse"))
                                .sorted(Comparator.comparingDouble(this::extractNumber))
                                .collect(Collectors.toList());
                        currentLevel.put("display_order", distanceSorted);
                    }
                } else {
                    List<String> order = List.of(formattedDistance);
                    currentLevel.put("display_order", order);
                }
            } else {
                currentLevel.put("-", result.getFormattedValue());
            }
        }

        return metadata;
    }

    void setCalculationProperties(Map<String, CalculationMethod> calculationMap) {
        SelectQuery query = Queries.SELECT();

        Variable calculation = query.var();
        Variable calculationType = query.var();
        Variable distanceVar = query.var();
        Variable datasetFilter = query.var();
        Variable filterColumnVar = query.var();
        Variable filterValueVar = query.var();

        ValuesPattern vPattern = new ValuesPattern(calculation,
                calculationMap.keySet().stream().map(k -> Rdf.iri(k)).collect(Collectors.toList()));

        GraphPattern gp1 = calculation.isA(calculationType);
        GraphPattern gp2 = calculation.has(HAS_DISTANCE, distanceVar).optional();

        GraphPattern gp3 = GraphPatterns.and(calculation.has(HAS_DATASET_FILTER, datasetFilter),
                datasetFilter.has(HAS_FILTER_COLUMN, filterColumnVar).andHas(HAS_FILTER_VALUE, filterValueVar))
                .optional();

        query.where(gp1, gp2, gp3, vPattern).prefix(PREFIX_EXPOSURE).distinct();

        JSONArray queryResult;
        if (Config.NAMESPACE != null) {
            queryResult = blazegraphClient.executeQuery(query.getQueryString());
        } else {
            queryResult = federateClient.executeQuery(query.getQueryString());
        }

        Set<String> queriedCalc = new HashSet<>();

        for (int i = 0; i < queryResult.length(); i++) {
            String calculationIri = queryResult.getJSONObject(i).getString(calculation.getVarName());
            queriedCalc.add(calculationIri);
            String calcType = queryResult.getJSONObject(i).getString(calculationType.getVarName());
            calcType = calcType.substring(calcType.lastIndexOf('/') + 1);
            calculationMap.get(calculationIri).setName(formatCalculationLabel(calcType));

            if (queryResult.getJSONObject(i).has(distanceVar.getVarName())) {
                double distance = queryResult.getJSONObject(i).getDouble(distanceVar.getVarName());
                calculationMap.get(calculationIri).setDistance(distance);
            }

            if (queryResult.getJSONObject(i).has(datasetFilter.getVarName())) {
                String filterColumn = queryResult.getJSONObject(i).getString(filterColumnVar.getVarName());
                String filterValue = queryResult.getJSONObject(i).getString(filterValueVar.getVarName());

                calculationMap.get(calculationIri).setDatasetFilter(filterColumn, filterValue);
            }
        }

        // this should not be needed unless previously there are deleted calculations
        // from blazegraph and the corresponding results are not removed from postgis
        if (queriedCalc.size() != calculationMap.size()) {
            LOGGER.warn("Some calculation instances in the exposure_results table are not present in the triple store");
        }

        calculationMap.keySet().retainAll(queriedCalc);
    }

    Map<String, String> getExposureName(Set<String> exposureSet) {
        SelectQuery query = Queries.SELECT();
        Variable exposureVar = query.var();
        Variable labelVar = query.var();

        ValuesPattern valuesPattern = new ValuesPattern(exposureVar,
                exposureSet.stream().map(k -> Rdf.iri(k)).collect(Collectors.toList()));

        query.where(exposureVar.has(RDFS.LABEL, labelVar), valuesPattern);

        JSONArray queryResult = federateClient.executeQuery(query.getQueryString());

        Map<String, String> exposureMap = new HashMap<>();
        for (int i = 0; i < queryResult.length(); i++) {
            String exposureIri = queryResult.getJSONObject(i).getString(exposureVar.getVarName());
            String label = queryResult.getJSONObject(i).getString(labelVar.getVarName());
            exposureMap.put(exposureIri, label);
        }

        return exposureMap;
    }

    /**
     * extracts number from strings like "400 m" or "400m"
     */
    private double extractNumber(String s) {
        Pattern numberPattern = Pattern.compile("(\\d+(?:\\.\\d+)?)");
        Matcher m = numberPattern.matcher(s);

        if (m.find()) {
            return Double.parseDouble(m.group(1));
        } else {
            String errmsg = "Failed to extract number from " + s;
            LOGGER.error(errmsg);
            throw new RuntimeException(errmsg);
        }
    }

    /**
     * with trips
     * 
     * @param iri
     * @param tripIndex
     * @return
     */
    JSONObject getResultsTrajectory(String iri, Integer tripIndex, String time) {
        // split into two queries due to performance issues
        // first query focuses more on time series data
        // second query gets the metadata of the results

        String query1;
        if (tripIndex != null) {
            try (InputStream is = QueryClient.class.getResourceAsStream("trajectory_query.sparql")) {
                query1 = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[TRIP_IRI]", getTripIri(iri)).replace(
                        "[TRIP_VALUE]", String.valueOf(tripIndex)).replace("[TIME_VALUE]", time);
            } catch (IOException e) {
                String errmsg = "Failed to process trajectory_query.sparql";
                LOGGER.error(errmsg);
                LOGGER.error(e.getMessage());
                throw new RuntimeException(errmsg, e);
            }
        } else {
            if (getTripIri(iri) != null) {
                String errmsg = "Trip instance detected, trip index must be provided in the request";
                LOGGER.error(errmsg);
                throw new RuntimeException(errmsg);
            }
            try (InputStream is = QueryClient.class.getResourceAsStream("query_without_trips.sparql")) {
                query1 = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[POINT_IRI]", iri)
                        .replace("[TIME_VALUE]", time);
            } catch (IOException e) {
                String errmsg = "Failed to process query_without_trips.sparql";
                LOGGER.error(errmsg);
                LOGGER.error(e.getMessage());
                throw new RuntimeException(errmsg, e);
            }
        }

        LOGGER.info("Executing time series query");
        JSONArray queryResult = federateClient.executeQuery(query1);

        if (queryResult.isEmpty()) {
            return new JSONObject();
        }

        Map<String, Double> resultToValueMap = new HashMap<>();

        for (int i = 0; i < queryResult.length(); i++) {
            // variable names are in trajectory_query.sparql in the resources folder
            String resultIri = queryResult.getJSONObject(i).getString("result");
            double exposureValue = queryResult.getJSONObject(i).getDouble("val");
            resultToValueMap.put(resultIri, exposureValue);
        }

        ValuesPattern values = new ValuesPattern(SparqlBuilder.var("result"),
                resultToValueMap.keySet().stream().map(r -> Rdf.iri(r)).collect(Collectors.toList()));

        String query2;
        try (InputStream is = QueryClient.class.getResourceAsStream("result_query.sparql")) {
            query2 = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[VALUES_CLAUSE]", values.getQueryString())
                    .replace("[SUBJECT]", iri);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Executing query to get metadata of results");
        JSONArray queryResult2 = federateClient.executeQuery(query2);

        if (queryResult2.length() != resultToValueMap.keySet().size()) {
            throw new RuntimeException("Unexpected query result size");
        }

        Map<String, Double> resultToDistanceMap = new HashMap<>();
        Map<String, String> resultToCalculationMap = new HashMap<>();
        Map<String, String> resultToUnitMap = new HashMap<>();
        Map<String, String> resultToDatasetMap = new HashMap<>();

        for (int i = 0; i < queryResult2.length(); i++) {
            String resultIri = queryResult2.getJSONObject(i).getString("result");
            String calculation = queryResult2.getJSONObject(i).getString("calculation_type");
            double distance = queryResult2.getJSONObject(i).getDouble("distance");
            String unit = queryResult2.getJSONObject(i).getString("unit");

            String datasetName;
            if (queryResult2.getJSONObject(i).has("exposure_dataset_name")) {
                datasetName = queryResult2.getJSONObject(i).getString("exposure_dataset_name");
            } else {
                datasetName = queryResult2.getJSONObject(i).getString("exposure_dataset");
            }

            resultToCalculationMap.put(resultIri, calculation);
            resultToDistanceMap.put(resultIri, distance);
            resultToUnitMap.put(resultIri, unit);
            resultToDatasetMap.put(resultIri, datasetName);
        }

        JSONObject metadata = new JSONObject();
        resultToValueMap.keySet().forEach(r -> {
            double exposureValue = resultToValueMap.get(r);
            double distance = resultToDistanceMap.get(r);
            String datasetName = resultToDatasetMap.get(r);
            String exposureUnit = resultToUnitMap.get(r);
            String calculationName = resultToCalculationMap.get(r);
            calculationName = formatCalculationLabel(
                    calculationName.substring(calculationName.lastIndexOf('/') + 1));
            String distanceKey = String.format("%.0f", distance) + " m";
            String formattedValue = String.format("%.0f %s", exposureValue,
                    exposureUnit == null || exposureUnit.isBlank() ? "[-]" : exposureUnit.strip());

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
        });

        return metadata;
    }

    /**
     * without trips
     */
    JSONObject getResultsTrajectory(String iri) {
        if (getTripIri(iri) != null) {
            String errmsg = "Trip instance detected, trip index must be provided in the request";
            throw new RuntimeException(errmsg);
        }
        JSONObject metadata = new JSONObject();

        return metadata;
    }

    String getTripIri(String trajectoryIri) {
        SelectQuery query = Queries.SELECT();
        Variable tripVar = query.var();
        Variable timeseriesVar = query.var();

        query.where(Rdf.iri(trajectoryIri).has(HAS_TIME_SERIES, timeseriesVar),
                tripVar.has(HAS_TIME_SERIES, timeseriesVar).andIsA(TRIP)).prefix(PREFIX_TIMESERIES, PREFIX_EXPOSURE);

        JSONArray queryResult = federateClient.executeQuery(query.getQueryString());

        if (queryResult.isEmpty()) {
            return null;
        }
        return queryResult.getJSONObject(0).getString(tripVar.getVarName());
    }

    /** Input bounds, stored observation times and output stay bounds use epoch seconds. */
    JSONArray getTimelineResults(String userId, double lower, double upper) {
        if (userId.isBlank() || userId.matches(".*[\\s<>\"{}|\\\\^`].*")) {
            throw new IllegalArgumentException("Token subject cannot form a user IRI");
        }
        String person = "https://w3id.org/MON/person.owl#person_" + userId;
        java.net.URI.create(person);
        SelectQuery ownership = Queries.SELECT();
        Variable point = SparqlBuilder.var("point");
        Variable device = ownership.var();
        Variable sensor = ownership.var();
        ownership.select(point).distinct().where(
                Rdf.iri(person).has(Rdf.iri("https://www.theworldavatar.com/kg/sensorloggerapp/hasA"), device),
                device.has(Rdf.iri("https://saref.etsi.org/core/consistsOf"), sensor),
                sensor.has(Rdf.iri("https://www.theworldavatar.com/kg/ontodevice/hasGeoLocation"), point));
        JSONArray points = federateClient.executeQuery(ownership.getQueryString());
        JSONArray observations = new JSONArray();
        String template;
        try (InputStream is = QueryClient.class.getResourceAsStream("trip_groups_query.sparql")) {
            template = IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read trip_groups_query.sparql", e);
        }
        for (int i = 0; i < points.length(); i++) {
            String pointIri = points.getJSONObject(i).getString("point");
            String tripIri = getTripIri(pointIri);
            if (tripIri == null) {
                continue; // A user's new point series may not have been processed yet.
            }
            JSONArray rows = federateClient.executeQuery(template.replace("[TRIP_IRI]", tripIri));
            for (int j = 0; j < rows.length(); j++) {
                observations.put(rows.getJSONObject(j).put("point", pointIri));
            }
        }
        JSONArray groups = groupTripObservations(observations, lower, upper);
        for (int i = 0; i < groups.length(); i++) {
            JSONObject group = groups.getJSONObject(i);
            JSONObject results = new JSONObject();
            JSONObject samples = group.getJSONObject("samples");
            for (String pointIri : samples.keySet()) {
                mergeTimelineResults(results, getResultsTrajectory(pointIri, group.getInt("trip"),
                        samples.get(pointIri).toString()));
            }
            group.remove("samples");
            group.remove("sample_time");
            group.put("results", results);
        }
        return groups;
    }

    static void mergeTimelineResults(JSONObject target, JSONObject source) {
        for (String key : source.keySet()) {
            Object value = source.get(key);
            if (!target.has(key)) {
                target.put(key, value);
            } else if (value instanceof JSONObject && target.get(key) instanceof JSONObject) {
                mergeTimelineResults(target.getJSONObject(key), (JSONObject) value);
            } else if (!target.get(key).equals(value)) {
                throw new IllegalStateException("Conflicting exposure results across point series for " + key);
            }
        }
    }

    JSONArray getResultsTrajectoryRange(String iri, double lower, double upper) {
        String tripIri = getTripIri(iri);
        if (tripIri == null) {
            throw new IllegalArgumentException("Trajectory must contain trip information");
        }
        String query;
        try (InputStream is = QueryClient.class.getResourceAsStream("trip_groups_query.sparql")) {
            query = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[TRIP_IRI]", tripIri);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read trip_groups_query.sparql", e);
        }
        // Read all trip observations before filtering to preserve full stay bounds and numbering.
        JSONArray groups = groupTripObservations(federateClient.executeQuery(query), lower, upper);
        for (int i = 0; i < groups.length(); i++) {
            JSONObject group = groups.getJSONObject(i);
            String sampleTime = group.getString("sample_time");
            group.remove("sample_time");
            group.put("results", getResultsTrajectory(iri, group.getInt("trip"), sampleTime));
        }
        // Preserve the chronological order established by groupTripObservations.
        return groups;
    }

    static JSONArray groupTripObservations(JSONArray observations, double lower, double upper) {
        List<JSONObject> rows = new ArrayList<>();
        for (int i = 0; i < observations.length(); i++) {
            rows.add(observations.getJSONObject(i));
        }
        rows.sort(Comparator.comparingDouble(row -> row.getDouble("time")));
        for (int i = 0; i < rows.size(); i++) {
            double time = rows.get(i).getDouble("time");
            if (!Double.isFinite(time)) {
                throw new IllegalStateException("Trip observation time must be finite");
            }
            if (i > 0 && time == rows.get(i - 1).getDouble("time")
                    && rows.get(i).getInt("trip") != rows.get(i - 1).getInt("trip")) {
                throw new IllegalStateException("Conflicting trip indices at time " + time);
            }
        }
        JSONArray groups = new JSONArray();
        int stay = 0;
        Set<String> keys = new HashSet<>();
        for (int first = 0; first < rows.size();) {
            int trip = rows.get(first).getInt("trip");
            int end = first + 1;
            while (end < rows.size() && rows.get(end).getInt("trip") == trip) {
                end++;
            }
            String key = trip == 0 ? "stay-" + (++stay) : "trip-" + trip;
            if (!keys.add(key)) {
                throw new IllegalStateException("Nonzero trip index occurs in multiple separate groups: " + trip);
            }
            double startSeconds = rows.get(first).getDouble("time");
            double endSeconds = rows.get(end - 1).getDouble("time");
            if (!Double.isFinite(startSeconds) || !Double.isFinite(endSeconds)) {
                throw new IllegalStateException("Trip observation time must be finite");
            }
            if (startSeconds <= upper && endSeconds >= lower) {
                JSONObject group = new JSONObject();
                group.put("key", key);
                group.put("trip", trip);
                // Results repeat within the group, so query only its first observation.
                group.put("sample_time", rows.get(first).get("time").toString());
                JSONObject samples = new JSONObject();
                for (int i = first; i < end; i++) {
                    JSONObject row = rows.get(i);
                    if (row.has("point") && !samples.has(row.getString("point"))) {
                        samples.put(row.getString("point"), row.get("time"));
                    }
                }
                if (!samples.isEmpty()) {
                    group.put("samples", samples);
                }
                if (trip == 0) {
                    group.put("lowerbound", startSeconds);
                    group.put("upperbound", endSeconds);
                }
                groups.put(group);
            }
            first = end;
        }
        return groups;
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
}
