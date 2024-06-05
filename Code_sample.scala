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
