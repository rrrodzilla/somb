const AWS = require('aws-sdk');
const ddbGeo = require('dynamodb-geo');

AWS
    .config
    .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

AWS
    .config
    .update({region: "us-west-1"});

exports.handler = async(event) => {

    let entity = event.entity;
    let entityKey = event.key;
    let entityLocation = JSON.parse(event.location);
    let params = JSON.parse(event.params);

    const ddb = new AWS.DynamoDB({endpoint: "dynamodb.us-west-1.amazonaws.com"});
    const config = new ddbGeo.GeoDataManagerConfiguration(ddb, entity);
    const myGeoTableManager = new ddbGeo.GeoDataManager(config);

    // Querying 100km from Cambridge, UK
    myGeoTableManager.queryRadius({
        RadiusInMeter: 100000,
        CenterPoint: {
            latitude: 52.225730,
            longitude: 0.149593
        }
    })
    // Print the results, an array of DynamoDB.AttributeMaps
        .then(console.log);

    myGeoTableManager.putPoint({
        RangeKeyValue: {
            S: entityKey
        }, // Use this to ensure uniqueness of the hash/range pairs.
        GeoPoint: { // An object specifying latitutde and longitude as plain numbers. Used to build the geohash, the hashkey and geojson data
            latitude: entityLocation.lat,
            longitude: entityLocation.lon
        },
            PutItemInput: { // Passed through to the underlying DynamoDB.putItem request. TableName is filled in for you.
                Item: params
            }
        })
        .promise()
        .then(function () {

            callback(null, null);

        })
        .catch((error) => {
            console.log("FAIL:");
            console.log(error);
            callback(error, null);
        });
};