const client = require('twilio')(process.env.ACCOUNTSID, process.env.AUTHTOKEN);
exports.handler = async(event) => {

    //let's parse this incoming record
    let message = JSON.parse(event.Records[0].body);
    let event_type = event.Records[0].messageAttributes["event.type"].stringValue;
    let flow_sid = event.Records[0].messageAttributes["event.flow"].stringValue;

    console.log("INCOMING RECORD.MESSAGE: ");
    console.log("EVENT TYPE: " + event_type);
    //need to parse twice since the json gets escaped twice

    console.log("message: ");
    console.log(message);

    if (event_type === "update.request.status.complete") {
        message.params.type = "open.request.cancelled";
    }

    console.log("trying studio flow call");
    try {
        var smsMsg = client
            .studio
            .flows(flow_sid)
            .executions
            .create({to: message.to, from: message.from, parameters: message.params});
        await smsMsg.then(() => {
            return;
        });
    } catch (err) {
        console.log("ERROR:");
        return err;
    }
};
