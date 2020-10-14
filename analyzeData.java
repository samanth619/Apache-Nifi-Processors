package data.processors.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "analyze", "data", "process", "analyse" })
@CapabilityDescription("This custom processor can be used for Analytics.")
@SeeAlso({})
public class analyzeData extends AbstractProcessor {
	private ComponentLog logger;

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("original")
			.description("Original input flowfiles").build();
	public static final Relationship REL_RESULT = new Relationship.Builder().name("Daily-Statistics")
			.description("Flowfiles that contains analytics result").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("FlowFiles that failed to be processed").build();
	public static final Relationship REL_LOC_STAT = new Relationship.Builder().name("Loc-Statistics")
			.description("FlowFiles that contains location statistics").build();

	public static final PropertyDescriptor PPT_KEY = new PropertyDescriptor.Builder().name("Location key")
			.displayName("Location Key")
			.description(
					"'+' seperated field names of input record that has to be taken for generating unique location key.")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;
	private ObjectMapper objectMapper = null;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(PPT_KEY);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_LOC_STAT);
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

		Map<String, JsonNode> output = new HashMap<String, JsonNode>();
		Map<String, JsonNode> Loc_stat_output = new HashMap<String, JsonNode>();
		Map<String, ArrayList<String>> users = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<String>> locations = new HashMap<String, ArrayList<String>>();
		String keyFields[] = context.getProperty(PPT_KEY).toString().split("\\+");

		objectMapper = new ObjectMapper();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(session.read(flowFile)))) {
			try {
				String line;
				FlowFile ff = session.create(flowFile);
				while ((line = br.readLine()) != null) {
					JsonNode node = objectMapper.readTree(line);

					String date = node.get("date").asText();
					String uid = node.get("CDR_UID").asText();
					String lon = node.get("lon").asText();
					String lat = node.get("lat").asText();
					String state = node.get("state").asText();
					String city = node.get("city").asText();
					String suburb = node.get("suburb").asText();

					String delim = "";
					StringBuilder keyBuilder = new StringBuilder();
					for (String field : keyFields) {
						keyBuilder.append(delim + node.get(field).asText());
						delim = "_";
					}

					String locKey = new String(keyBuilder);
					JsonNode entry = objectMapper.createObjectNode();

					// Initialize data
					if (output.containsKey(date)) {
						entry = output.get(date);
					} else {
						((ObjectNode) entry).put("num_calls", 0);
						((ObjectNode) entry).put("num_location", 0);
						((ObjectNode) entry).put("unique_users", 0);
						((ObjectNode) entry).put("total_duration", 0);
					}

					// Number of calls calculation
					int num_calls = entry.get("num_calls").asInt();
					num_calls += 1;
					((ObjectNode) entry).put("num_calls", num_calls);

					// Unique user calculation
					if (!users.containsKey(date)) {
						users.put(date, new ArrayList<String>());
					}
					if (!users.get(date).contains(uid)) {
						int user_count = entry.get("unique_users").asInt();
						user_count += 1;
						((ObjectNode) entry).put("unique_users", user_count);
						users.get(date).add(uid);
					}

					// Total duration calculation
					int duration = entry.get("total_duration").asInt();
					duration += Integer.parseInt(node.get("duration").asText());
					((ObjectNode) entry).put("total_duration", duration);
					output.put(date, entry);

					// Unique location calculation
					if (!locations.containsKey(date)) {
						locations.put(date, new ArrayList<String>());
					}
					if (!locations.get(date).contains(locKey)) {
						int loc_count = entry.get("num_location").asInt();
						loc_count += 1;
						((ObjectNode) entry).put("num_location", loc_count);
						locations.get(date).add(locKey);
					}

					// Location Statistics calculation
					JsonNode Loc_stat = objectMapper.createObjectNode();
					if (Loc_stat_output.containsKey(date)) {
						Loc_stat = Loc_stat_output.get(date);
					}
					if (Loc_stat.has(locKey)) {
						ObjectNode locJSON = (ObjectNode) Loc_stat.get(locKey);
						int loc_cnt = locJSON.get("count").asInt();
						loc_cnt += 1;
						locJSON.put("count", loc_cnt);
					} else {
						ObjectNode newLocData = objectMapper.createObjectNode();
						newLocData.put("count", 1);
						newLocData.put("lon", lon);
						newLocData.put("lat", lat);
						newLocData.put("state", state);
						newLocData.put("city", city);
						newLocData.put("suburb", suburb);
						((ObjectNode) Loc_stat).put(locKey, newLocData);
					}
					Loc_stat_output.put(date, Loc_stat);

				}

				// Write analytics output to flowfile
				for (String key : output.keySet()) {
					ObjectNode opt = (ObjectNode) output.get(key);
					opt.put("_id", key);
					System.out.println(opt);
					String outstr = opt.toString() + "\n";
					ff = session.append(ff, new OutputStreamCallback() {
						@Override
						public void process(OutputStream outp) throws IOException {
							outp.write(outstr.getBytes());
						}
					});
				}

				// Write Location statistics to flowfile
				FlowFile LocStatFlowFile = session.create(flowFile);
				for (String key : Loc_stat_output.keySet()) {
					ObjectNode opt = (ObjectNode) Loc_stat_output.get(key);
					opt.put("_id", key);
					System.out.println(opt);
					String outstr = opt.toString() + "\n";
					LocStatFlowFile = session.append(LocStatFlowFile, new OutputStreamCallback() {
						@Override
						public void process(OutputStream outp) throws IOException {
							outp.write(outstr.getBytes());
						}
					});
				}

				br.close();
				session.transfer(LocStatFlowFile, REL_LOC_STAT);
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
