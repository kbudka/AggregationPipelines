package mongotest.core;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Connection {
	private MongoClient mongoClient = null;
	private DB db;
	private DBCollection data;
	
	public Connection() {
		try {
			mongoClient = new MongoClient("127.0.0.1" , 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		db = mongoClient.getDB( "test" );
		data = db.getCollection("data");
	}
	
	public DB getDb() {
		return db;
	}

	public DBCollection getData() {
		return data;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

}
