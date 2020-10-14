package data.processors.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "Parse", "data", "process", "analyse", "csv" })
@CapabilityDescription("This custom processor can be used to parse CSV to JSON")
@SeeAlso({})
public class parseCSV extends AbstractProcessor {
	private ComponentLog logger;

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("original")
			.description("Original input flowfiles").build();
	public static final Relationship REL_RESULT = new Relationship.Builder().name("Parsed_JSON")
			.description("Flowfiles that contains parsed data").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("FlowFiles that failed to be processed").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;
	private ObjectMapper objectMapper = null;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
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

				ObjectNode node = objectMapper.createObjectNode();
				node.put("CDR_UID", fields[0]);
				node.put("datetime", fields[1]);
				node.put("date", fields[1].split(" ")[0]);
				node.put("duration", fields[2]);
				node.put("antcode", fields[3]);
				node.put("lon", fields[4]);
				node.put("lat", fields[5]);
				node.put("attri", fields[6]);

				String outstr = node.toString() + "\n";

				ff = session.append(ff, new OutputStreamCallback() {
					@Override
					public void process(OutputStream outp) throws IOException {
						outp.write(outstr.getBytes());
					}
				});

			}
			br.close();
			session.transfer(ff, REL_RESULT);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (Exception e) {
			logger.error("Error processing data : ", e);
			session.transfer(flowFile, REL_FAILURE);
		}
	}
}
