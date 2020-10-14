package data.processors.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "enrich", "data", "process", "analyse", "geo" })
@CapabilityDescription("This custom processor can be used for geo enrichment")
@SeeAlso({})
@Stateful(scopes = Scope.CLUSTER, description = "Stores Geo-Data.")

public class geoEnrich extends AbstractProcessor {
	private ComponentLog logger;

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("original")
			.description("Original input flowfiles").build();
	public static final Relationship REL_RESULT = new Relationship.Builder().name("Enriched_JSON")
			.description("Flowfiles that contains enriched data").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("FlowFiles that failed to be processed").build();

	public static final PropertyDescriptor PPT_SERVER = new PropertyDescriptor.Builder().name("ReverseGeo Server")
			.displayName("ReverseGeo server name").description("ReverseGeo server address withput 'http://' ")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;
	private ObjectMapper objectMapper = null;
	Map<String, String> output;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(PPT_SERVER);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_RESULT);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) throws IOException {
		logger = getLogger();
	}

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
		return new PropertyDescriptor.Builder().name(propertyDescriptorName)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).dynamic(true).build();
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		try {
			final StateManager stateManager = context.getStateManager();
			output = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
		} catch (Exception e) {
			output = new HashMap<String, String>();
		}
		objectMapper = new ObjectMapper();
		String server = context.getProperty(PPT_SERVER).toString();
		final String host = "http://" + server;

		try (BufferedReader br = new BufferedReader(new InputStreamReader(session.read(flowFile)))) {
			try {
				String line;
				FlowFile ff = session.create(flowFile);
				long count = 0;
				while ((line = br.readLine()) != null) {

					ObjectNode node = (ObjectNode) objectMapper.readTree(line);

					String locKey = node.get("lat").asDouble() + "_" + node.get("lon").asDouble();
					if (output.containsKey(locKey)) {
						ObjectNode resJson = (ObjectNode) objectMapper.readTree(output.get(locKey));
						node.put("city", resJson.get("city").asText());
						node.put("country", resJson.get("country").asText());
						node.put("country_code", resJson.get("country_code").asText());
						node.put("postcode", resJson.get("postcode").asText());
						node.put("state", resJson.get("state").asText());
						node.put("suburb", resJson.get("suburb").asText());
					} else {
						Double lat = node.get("lat").asDouble();
						Double lon = node.get("lon").asDouble();
						String urlString = host + "?lat=" + String.valueOf(lat) + "&lon=" + String.valueOf(lon);
						ObjectNode resJson = objectMapper.createObjectNode();
						try {
							URL url = new URL(urlString);
							HttpURLConnection conn = (HttpURLConnection) url.openConnection();
							conn.setRequestMethod("GET");
							conn.addRequestProperty("User-Agent", "Mozilla/4.76");

							BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
							String text;
							StringBuilder result = new StringBuilder();
							while ((text = in.readLine()) != null)
								result.append(text);

							in.close();
							resJson = (ObjectNode) objectMapper.readTree(result.toString());
						} catch (Exception e) {
							logger.error("exception : ", e);
						}

						node.put("city", resJson.get("city").asText());
						node.put("country", resJson.get("country").asText());
						node.put("country_code", resJson.get("country_code").asText());
						node.put("postcode", resJson.get("postcode").asText());
						node.put("state", resJson.get("state").asText());
						node.put("suburb", resJson.get("suburb").asText());
						output.put(locKey, resJson.toString());
						count++;
					}
					String outstr = node.toString() + "\n";

					ff = session.append(ff, new OutputStreamCallback() {
						@Override
						public void process(OutputStream outp) throws IOException {
							outp.write(outstr.getBytes());
						}
					});

					if (count > 100) {
						StateManager stateManager = context.getStateManager();
						stateManager.clear(Scope.CLUSTER);
						stateManager.setState(output, Scope.CLUSTER);
						count = 0;
					}
				}

				br.close();
				session.transfer(ff, REL_RESULT);
				session.transfer(flowFile, REL_SUCCESS);
			} catch (Exception e) {

				logger.error("Error processing data : ", e);
				session.transfer(flowFile, REL_FAILURE);
			}
		} catch (Exception e) {
			logger.error("Error processing data : ", e);
			session.transfer(flowFile, REL_FAILURE);
		}
	}
}
