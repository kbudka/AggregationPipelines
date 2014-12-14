package mongotest.core;

import java.util.Arrays;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class AggregationPipeline {
	Connection conn = new Connection();
	DB db = conn.getDb();
	DBCollection data = conn.getData();

	public static void main(String[] args) {
		AggregationPipeline app = new AggregationPipeline();
		
		for (DBObject result : app.getCountryPopulation()
				.results()) {
			System.out.println(result);
		}

	}

	public AggregationOutput getCountryPopulation() {
		DBObject fields = new BasicDBObject("country", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);

		DBObject groupFields = new BasicDBObject("_id", "$country");
		groupFields.put("Quantity", new BasicDBObject("$sum", 1));
		
		DBObject group = new BasicDBObject("$group", groupFields);

		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("country", 1));

		List<DBObject> pipeline = Arrays.asList(project, group, sort);
		AggregationOutput output = data.aggregate(pipeline);

		return output;
	}

	public AggregationOutput getMostGivenName() {
		DBObject fields = new BasicDBObject("first_name", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);

		DBObject groupFields = new BasicDBObject("_id", "$first_name");
		groupFields.put("Quantity", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);

		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("first_name", 1));

		List<DBObject> pipeline = Arrays.asList(project, group, sort);
		AggregationOutput output = data.aggregate(pipeline);

		return output;
	}

	public AggregationOutput getSurnameCountryPopulation() {
		DBObject project = new BasicDBObject("$project", new BasicDBObject("_id", 0)
			.append("Surname", "$last_name")
			.append("Country", "$country")
		);

		DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("Surname", "$Surname")
			.append("Country", "$Country"))
			.append("Quantity", new BasicDBObject("$sum", 1))
		);

		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("Quantity", 1));

		List<DBObject> pipeline = Arrays.asList(project, group, sort);
		AggregationOutput output = data.aggregate(pipeline);

		return output;
	}

	public AggregationOutput getAmazonDomainByCountryPopulation() {
		DBObject project = new BasicDBObject("$project", new BasicDBObject("_id", 0)
			.append("Country", "$country")
			.append("Email", "$email")
		);

		DBObject match = new BasicDBObject("$match", new BasicDBObject("Email", new BasicDBObject("$regex", ".*")));

		DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("Country", "$Country")
			.append("Email", "$Email"))
			.append("Quantity", new BasicDBObject("$sum",1))
		);

		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("Quantity", 1));

		List<DBObject> pipeline = Arrays.asList(project, match, group, sort);
		AggregationOutput output = data.aggregate(pipeline);

		return output;
	}

}
