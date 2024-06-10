object SqlQueryValidator {
  def containsOnlyAllowedAggregates(query: String): Boolean = {
    // Define a regex to match any function in the query
    val functionPattern = "(?i)\\b([A-Z_][A-Z0-9_]*)\\s*\\(".r

    // Find all matches of functions in the query
    val matches = functionPattern.findAllIn(query).toList

    // Define the allowed set of functions
    val allowedFunctions = Set("SUM", "AVG", "MIN", "MAX")

    // Extract the function names and check if they are all within the allowed set
    matches.forall { matchStr =>
      val functionName = matchStr.split("\\(")(0).trim.toUpperCase
      allowedFunctions.contains(functionName)
    }
  }

  def main(args: Array[String]): Unit = {
    // Test cases
    val testQuery1 = "SELECT SUM(amount), AVG(price) FROM sales"
    val testQuery2 = "SELECT COUNT(id), SUM(amount) FROM sales"
    val testQuery3 = "SELECT MIN(age), MAX(score), AVG(height) FROM students"
    val testQuery4 = "SELECT SUM(amount), AVG(price), MIN(date), MAX(id) FROM sales"
    val testQuery5 = "SELECT name, address FROM customers"
    val testQuery6 = "SELECT SUM(amount) AS total FROM sales"
    val testQuery7 = "SELECT SUM(amount), AVG(price), Veera(price) FROM sales"
    
    println(containsOnlyAllowedAggregates(testQuery1)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery2)) // Should print: false
    println(containsOnlyAllowedAggregates(testQuery3)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery4)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery5)) // Should print: false
    println(containsOnlyAllowedAggregates(testQuery6)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery7)) // Should print: false
  }
}

