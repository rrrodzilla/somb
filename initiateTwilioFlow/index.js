const client = require('twilio')(process.env.ACCOUNTSID, process.env.AUTHTOKEN);
const FLOWS = {
    "initiate.twilio.flow": "FW5aa2094eef2a5efade132ed9a4998d6f",
    "new.incoming.message": "FW5aa2094eef2a5efade132ed9a4998d6f",
    "open.issue.cancelled": "FW5aa2094eef2a5efade132ed9a4998d6f",
    "open.issue.found": "FW5aa2094eef2a5efade132ed9a4998d6f"
};
exports.handler = async(event) => {

    var record = JSON.parse(event.Records[0].body);
    let event_type = record.MessageAttributes["event.type"].Value;
    let flow_sid = record.MessageAttributes["flow.sid"].Value;

    console.log("INCOMING RECORD.MESSAGE: ");
    console.log("EVENT TYPE: " + event_type);

    //need to parse twice since the json gets escaped twice
    let msg_org = JSON.parse(record.Message);
    let msg_obj = msg_org;
    console.log("msg_obj: ");
    console.log(msg_obj);

    if (event_type == "request.status.updated") 
        msg_obj.params.type = "open.request.cancelled";
    
    console.log("trying studio flow call");
    try {
        var smsMsg = client
            .studio
            .flows(flow_sid)
            .executions
            .create({to: msg_obj.to, from: msg_obj.from, parameters: msg_obj.params});
        await smsMsg.then(execution => {
            return;
        });
    } catch (err) {
        console.log("ERROR:");
        return err;
    }
};
