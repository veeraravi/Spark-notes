object SqlQueryValidator {
  def containsOnlyAllowedAggregates(query: String): Boolean = {
    // Define a regex to match the aggregate functions SUM, AVG, MIN, and MAX
    val aggregateFunctionPattern = "(?i)\\b(SUM|AVG|MIN|MAX)\\b".r

    // Find all matches of the aggregate functions in the query
    val matches = aggregateFunctionPattern.findAllIn(query).toList

    // Check if all found functions are within the allowed set
    val allowedFunctions = Set("SUM", "AVG", "MIN", "MAX")

    matches.forall(func => allowedFunctions.contains(func.toUpperCase))
  }

  def main(args: Array[String]): Unit = {
    // Test cases
    val testQuery1 = "SELECT SUM(amount), AVG(price) FROM sales"
    val testQuery2 = "SELECT COUNT(id), SUM(amount) FROM sales"
    val testQuery3 = "SELECT MIN(age), MAX(score), AVG(height) FROM students"
    val testQuery4 = "SELECT SUM(amount), AVG(price), MIN(date), MAX(id) FROM sales"
    
    println(containsOnlyAllowedAggregates(testQuery1)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery2)) // Should print: false
    println(containsOnlyAllowedAggregates(testQuery3)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery4)) // Should print: true
  }
}



object SqlQueryValidator {
  def containsOnlyAllowedAggregates(query: String): Boolean = {
    // Define a regex to match the aggregate functions SUM, AVG, MIN, and MAX
    val aggregateFunctionPattern = "(?i)\\b(SUM|AVG|MIN|MAX)\\b".r
    // Define a regex to match any SQL function or word
    val anyFunctionPattern = "(?i)\\b([A-Z_][A-Z0-9_]*)\\b".r

    // Find all matches of the aggregate functions in the query
    val matches = aggregateFunctionPattern.findAllIn(query).toList

    // If no aggregate functions are found, return false
    if (matches.isEmpty) return false

    // Find all SQL functions or words in the query
    val allFunctions = anyFunctionPattern.findAllIn(query).toList

    // Check if all found functions are within the allowed set
    val allowedFunctions = Set("SUM", "AVG", "MIN", "MAX")

    allFunctions.forall(func => allowedFunctions.contains(func.toUpperCase))
  }

  def main(args: Array[String]): Unit = {
    // Test cases
    val testQuery1 = "SELECT SUM(amount), AVG(price) FROM sales" // Contains only allowed aggregates
    val testQuery2 = "SELECT COUNT(id), SUM(amount) FROM sales"  // Contains COUNT, which is not allowed
    val testQuery3 = "SELECT MIN(age), MAX(score), AVG(height) FROM students" // Contains only allowed aggregates
    val testQuery4 = "SELECT SUM(amount), AVG(price), MIN(date), MAX(id) FROM sales" // Contains only allowed aggregates
    val testQuery5 = "SELECT name, address FROM customers" // Contains no aggregate functions
    val testQuery6 = "SELECT SUM(amount) AS total FROM sales" // Alias should not affect the check

    println(containsOnlyAllowedAggregates(testQuery1)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery2)) // Should print: false
    println(containsOnlyAllowedAggregates(testQuery3)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery4)) // Should print: true
    println(containsOnlyAllowedAggregates(testQuery5)) // Should print: false
    println(containsOnlyAllowedAggregates(testQuery6)) // Should print: true
  }
}

