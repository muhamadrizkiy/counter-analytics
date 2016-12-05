var moment = require('moment');
var mongojs = require('mongojs');

// ('database name',['source DB', 'result DB'])
var db = mongojs('localhost:57017/cyclone_statistic', ['data', 'yearly_music_premium']);

// get arguments value
var args = process.argv[2];

// set current time
var date = moment(args);

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

var mapper = function () {
    var value = {
        count: 1,
        data : {}
    };
    value.data[this.contentId.valueOf()] = {
        count: 1
    };
    var day = new Date(this.ts.getFullYear(),
        0,0,0,0,0,0)
    emit(day, value);
};

// reduce
var reducer = function(day, values) {
    var result;
    result = {
        count: 0,
        data: {}
    };
    var datas = [];
    for (i = 0; i < values.length; i++) {
        var contentId;
        result.count += values[i].count;
        for (contentId in values[i]['data']) {
            result.data[contentId] = result.data[contentId] || {count: 0};
            result.data[contentId].count += values[i].data[contentId].count;
        }
    }
    for (contentId in result.data) {
        var tmp = {};
        tmp.contentId = contentId;
        tmp.count = result.data[contentId].count;
        datas.push(tmp);
    }
    result.data = datas;
    return result;
}

// map reduce
db.data.mapReduce(
    mapper,
    reducer,
    {
        out: "yearly_music_premium",
        query: {
            // ts: {
            //     $gte: new Date(newTimeA),
            //     $lt: new Date(newTimeB)
            // }
            contentType : 'Music',
            membershipStatus : 'premium'
        }
    }
);

// view output
db.yearly_music_premium.find(function (err, docs) {
    if(err) console.log(err);
    console.log(docs);
});