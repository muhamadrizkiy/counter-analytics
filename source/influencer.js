var moment = require('moment');
var mongojs = require('mongojs');

// ('database name',['source DB', 'result DB'])
var db = mongojs('localhost:57017/cyclone_statistic', ['Membership', 'monthly_membership']);

// get arguments value
var args = process.argv[2];

// set current time
var date = moment(args);
var today = new Date();

// substract time
var myString = "01:00:00",
    myStringParts = myString.split(':'),
    hourDelta = myStringParts[0],
    minuteDelta= myStringParts[1];
var newDate = date.subtract({ hours: hourDelta, minutes: minuteDelta});

// set time for query
var timeA = moment(newDate);
var timeB = moment(args);
var newTimeA = timeA.toISOString();
var newTimeB = timeB.toISOString();

console.log(newTimeA,newTimeB);

// data mapping
var mapper = function () {
    var value = {
        count: 1,
        data : {}
    };
    value.data = this._id;
    var day = new Date(this.startDate.getFullYear(),
        this.startDate.getMonth(),
        0,0,0,0,0);
    emit(day, value);
};

// reduce
var reducer = function(day, values) {
    var result;
    result = {
        count: 0,
        data: {}
    };
    // var datas = [];
    for (i = 0; i < values.length; i++) {
        var gender;
        result.count += values[i].count;
        for (gender in values[i]) {
            result.data = values[i]['data'];
            // result.data[gender].count += values[i].data[gender].count;
        }
    }
    // for (gender in result.data) {
    //     var tmp = {};
    //     tmp.gender = gender;
    //     tmp.count = result.data[gender].count;
    //     datas.push(tmp);
    // }
    // result.data = datas;
    return result;
}

// map reduce
db.Membership.mapReduce(
    mapper,
    reducer,
    {
        out: "monthly_membership",
        query: {
            // ts: {
            //     $gte: new Date(newTimeA),
            //     $lt: new Date(newTimeB)
            // }
            // current: {$gte:30000},
            // contentType : 'Music',
            // membershipStatus : 'premium'
            packageType : 'premium'
        }
    }
);

// view output
db.monthly_membership.find(function (err, docs) {
    if(err) console.log(err);
    console.log(docs);
});