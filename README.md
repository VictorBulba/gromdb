# ⚡️ GromDB ⚡️

A powerful name, isn't it? As was my ambition when I wrote this code.

Simple embedded database. It internally uses the RobinHood hashmap algorithm to store indexes for a data section.

Don't take it seriously, I was practicing Golang. I am publishing this 2 years after it was created. It seems to work, but needs improvement in scaling, caching, API design and abstractions.

## Example

```go
var db, _ = gromdb.Open("test")
defer db.Close()

db.PutObj(12345, "One two three four five")
db.PutObj(54321, "Five four three two one")
db.PutObj(11111, "One one one one one")

var secondValue string
db.GetObj((54321), &secondValue)
var thirdValue string
db.GetObj((11111), &thirdValue)
var firstValue string
db.GetObj((12345), &firstValue)
```
