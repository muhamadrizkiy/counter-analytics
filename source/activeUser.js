var moment = require('moment');
var mongojs = require('mongojs');

// ('database name',['source DB', 'result DB'])
var db = mongojs('localhost:57017/cyclone_statistic', ['data', 'monthly_influencer']);

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
    value.data[this.contentId.valueOf()] = {
        sourceContentId : this.sourceContentId.valueOf(),
        sourceContentType : this.sourceContentType,
        count: 1
    };
    var day = new Date(this.ts.getFullYear(),
        this.ts.getMonth(),
        0,0,0,0,0);
    emit(day, value);
};

// reduce
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
            result.data[contentId].sourceContentId = values[i].data[contentId].sourceContentId;
            result.data[contentId].sourceContentType = values[i].data[contentId].sourceContentType;
        }
    }
    for (contentId in result.data) {
        var tmp = {};
        tmp.contentId = contentId;
        tmp.count = result.data[contentId].count;
        tmp.sourceContentId = result.data[contentId].sourceContentId;
        tmp.sourceContentType = result.data[contentId].sourceContentType;
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
        out: "monthly_influencer",
        query: {
        //     // ts: {
        //     //     $gte: new Date(newTimeA),
        //     //     $lt: new Date(newTimeB)
        //     // }
        //     // current: {$gte:30000},
        //     // contentType : 'Music',
        //     // membershipStatus : 'premium'
            $or: [
                { sourceContentType: 'Playlist' },
                { sourceContentType: 'Feed' },
                { sourceContentType: 'Upload' }
            ]
        }
    }
);

// view output
db.monthly_influencer.find(function (err, docs) {
    if(err) console.log(err);
    console.log(docs);
});