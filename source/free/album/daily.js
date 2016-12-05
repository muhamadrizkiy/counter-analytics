var moment = require('moment');
var mongojs = require('mongojs');

// ('database name',['source DB', 'result DB'])
var db = mongojs('localhost:57017/cyclone_statistic', ['data', 'daily_album_free']);

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

// data mapping
var mapper = function () {
    var value = {
        count: 1,
        data : {}
    };
    value.data[this.albumId.valueOf()] = {
        count: 1
    };
    var day = new Date(this.ts.getFullYear(),
        this.ts.getMonth(),
        this.ts.getDate(),
        0,0,0,0)
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
        var albumId;
        result.count += values[i].count;
        for (albumId in values[i]['data']) {
            result.data[albumId] = result.data[albumId] || {count: 0};
            result.data[albumId].count += values[i].data[albumId].count;
        }
    }
    for (albumId in result.data) {
        var tmp = {};
        tmp.albumId = albumId;
        tmp.count = result.data[albumId].count;
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
        out: "daily_album_free",
        query: {
            // ts: {
            //     $gte: new Date(newTimeA),
            //     $lt: new Date(newTimeB)
            // }
            contentType : 'Music',
            membershipStatus : 'free'
        }
    }
);

// view output
db.daily_album_free.find(function (err, docs) {
    if(err) console.log(err);
    console.log(docs);
});