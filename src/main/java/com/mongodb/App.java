package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.pull;

/**
 * Hello world!
 */
public class App {
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("school");
		MongoCollection<Document> students = db.getCollection("students");

		Bson filter = eq("type", "homework");
		Bson projection = fields(include("student_id"), include("score"), excludeId());
		Bson sort = ascending("student_id", "score");

		MongoCursor<Document> cursor = students.find().iterator();

		try {
			while (cursor.hasNext()) {
				Document currentStudent = cursor.next();
				Document lowestHomeworkScore = new Document("score", Double.MAX_VALUE);
				List<Document> scores = (List<Document>) currentStudent.get("scores");

				for (Document currentScore : scores) {
					if (currentScore.getString("type").equals("homework")) {
						if (currentScore.getDouble("score") <= lowestHomeworkScore.getDouble("score")) {
							lowestHomeworkScore = currentScore;
						}
					}
				}
				if (lowestHomeworkScore != null) {
					scores.remove(lowestHomeworkScore);
					students.updateOne(eq("_id", currentStudent.getInteger("_id")), pull("scores", lowestHomeworkScore));
				}
			}
		} finally {
			cursor.close();
		}
	}
}
