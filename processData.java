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
@Tags({ "Process", "data", "process", "analyse" })
@CapabilityDescription("This custom processor can be used for Analytics.")
@SeeAlso({})
public class processData extends AbstractProcessor {
	private ComponentLog logger;

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("original")
			.description("Original input flowfiles").build();
	public static final Relationship REL_RESULT = new Relationship.Builder().name("Daily-Statistics")
			.description("Flowfiles that contains analytics result").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("FlowFiles that failed to be processed").build();
	public static final Relationship REL_ANT_STAT = new Relationship.Builder().name("Loc-Statistics")
			.description("FlowFiles that contains antenna statistics").build();
	public static final Relationship REL_ANT_LOC = new Relationship.Builder().name("Ant-Loc-Info")
			.description("FlowFiles that contains antenna-location info").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;
	private ObjectMapper objectMapper = null;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_ANT_STAT);
		relationships.add(REL_ANT_LOC);
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
		Map<String, JsonNode> Ant_stat_output = new HashMap<String, JsonNode>();
		Map<String, ArrayList<String>> users = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<String>> locations = new HashMap<String, ArrayList<String>>();
		Map<String, ObjectNode> antCodeInfo = new HashMap<String, ObjectNode>();

		objectMapper = new ObjectMapper();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(session.read(flowFile)))) {
			String line;
			FlowFile ff = session.create(flowFile);
			int lcnt = 0;
			while ((line = br.readLine()) != null) {
				if (lcnt == 0) {
					if (line.contains("CDR_UID")) {
						lcnt++;
						continue;
					}
				}
				lcnt = 1;
				// Split data using comma
				String fields[] = line.split(",");

				// validate input data
				if (fields.length != 7) {
					logger.error("Invalid length: " + fields.length + " - " + line);
					continue;
				}

				String date = fields[1].split(" ")[0];
				JsonNode entry = objectMapper.createObjectNode();

				// Initialize data
				if (output.containsKey(date)) {
					entry = output.get(date);
				} else {
					((ObjectNode) entry).put("num_calls", 0);
					((ObjectNode) entry).put("num_ant_loc", 0);
					((ObjectNode) entry).put("unique_users", 0);
					((ObjectNode) entry).put("total_duration", 0);
				}

				// ANT-Location Statistics calculation
				JsonNode ant_stat = objectMapper.createObjectNode();
				if (Ant_stat_output.containsKey(date)) {
					ant_stat = Ant_stat_output.get(date);
				}
				if (ant_stat.has(fields[3])) {
					int ant_cnt = ant_stat.get(fields[3]).asInt();
					ant_cnt += 1;
					((ObjectNode) ant_stat).put(fields[3], ant_cnt);
				} else {
					((ObjectNode) ant_stat).put(fields[3], 1);
				}
				Ant_stat_output.put(date, ant_stat);

				// Unique user calculation
				if (!users.containsKey(date)) {
					users.put(date, new ArrayList<String>());
				}
				if (!users.get(date).contains(fields[0])) {
					int user_count = entry.get("unique_users").asInt();
					user_count += 1;
					((ObjectNode) entry).put("unique_users", user_count);
					users.get(date).add(fields[0]);
				}

				// Unique location calculation
				if (!locations.containsKey(date)) {
					locations.put(date, new ArrayList<String>());
				}
				if (!locations.get(date).contains(fields[3])) {
					int loc_count = entry.get("num_ant_loc").asInt();
					loc_count += 1;
					((ObjectNode) entry).put("num_ant_loc", loc_count);
					locations.get(date).add(fields[3]);
				}

				// Number of calls calculation
				int num_calls = entry.get("num_calls").asInt();
				num_calls += 1;
				((ObjectNode) entry).put("num_calls", num_calls);

				// Total duration calculation
				int duration = entry.get("total_duration").asInt();
				duration += Integer.parseInt(fields[2]);
				((ObjectNode) entry).put("total_duration", duration);
				output.put(date, entry);

				// ANTCODE - Location data
				if (!antCodeInfo.containsKey(fields[3])) {
					ObjectNode antloc = objectMapper.createObjectNode();
					antloc.put("lon", fields[4]);
					antloc.put("lat", fields[5]);
					antCodeInfo.put(fields[3], antloc);
				}
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

			// Write ANT-Location to flowfile
			FlowFile antLocFlowFile = session.create(flowFile);
			for (String key : antCodeInfo.keySet()) {
				ObjectNode opt = antCodeInfo.get(key);
				opt.put("_id", key);
				System.out.println(opt);
				String outstr = opt.toString() + "\n";
//				logger.error("data :" + outstr);
				antLocFlowFile = session.append(antLocFlowFile, new OutputStreamCallback() {
					@Override
					public void process(OutputStream outp) throws IOException {
						outp.write(outstr.getBytes());
					}
				});
			}

			// Write ANT-Location statistics to flowfile
			FlowFile antStatFlowFile = session.create(flowFile);
			for (String key : Ant_stat_output.keySet()) {
				ObjectNode opt = (ObjectNode) Ant_stat_output.get(key);
				opt.put("_id", key);
				System.out.println(opt);
				String outstr = opt.toString() + "\n";
				antStatFlowFile = session.append(antStatFlowFile, new OutputStreamCallback() {
					@Override
					public void process(OutputStream outp) throws IOException {
						outp.write(outstr.getBytes());
					}
				});
			}

			br.close();
			session.transfer(antLocFlowFile, REL_ANT_LOC);
			session.transfer(antStatFlowFile, REL_ANT_STAT);
			session.transfer(ff, REL_RESULT);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (Exception e) {
			logger.error("Error processing data : ", e);
			session.transfer(flowFile, REL_FAILURE);
		}
	}
}
