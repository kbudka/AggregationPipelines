
//1. Populacja państw posortowana po nazwie
// countryPopulationOrder.json
var getCountryPopulation = db.data.aggregate(
  [
    { $project :  { Country : "$country" } },
    { $group : { _id : {Country:"$Country"} , Quantity : { $sum : 1 } } },
    { $sort : { "_id.Country" : 1 } }
  ]
); 

while(getcountryPopulation.hasNext()) {
	printjson(getcountryPopulation.next())
}



//2. Najczęściej występujące imie
// mostGivenName.json
var getMostGivenName = db.data.aggregate(
  [
    { $project :  { FirstName : "$first_name" } },
    { $group : { _id : {FirstName:"$FirstName"} , Quantity : { $sum : 1 } } },
    { $sort : { Quantity : -1 } },
    { $limit : 1 }
  ]
);

printjson(getMostGivenName.next())



//3. Ilość pojedyńczych nazwisk w poszczególnych Państwach
// surnameCountryPopulation.json
var getSurnameCountryPopulation = db.data.aggregate(
  [
    { $project :  { Country : "$country" , Surname : "$last_name" } },
    { $group : { _id : {Surname:"$Surname", Country : "$Country"} , Quantity : { $sum : 1 } } },
    { $sort : { "_id.Country" : 1 } }
  ]
); 

while(getSurnameCountryPopulation.hasNext()) {
	printjson(getSurnameCountryPopulation.next())
}



//4. Ilość użytkowników posiadających maila na domenie Amazon w poszczególnych miastach
// amazonDomainCountryPopulation.json
var getAmazonDomainByCountryPopulation = db.data.aggregate(
  [
    { $project :  { Country : "$country" , email : "$email" } },
    { $match : { email : { $regex : /.*amazon.*/ } } },
    { $group : { _id : {Country : "$Country"} , Quantity : { $sum : 1 } } },
    { $sort : { "_id.Country" : 1 } }
  ]
); 

while(getAmazonDomainByCountryPopulation.hasNext()) {
	printjson(getAmazonDomainByCountryPopulation.next())
}