var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');
var app = express();

app.use(bodyParser.json());

var timeserie = require('./series');
var countryTimeseries = require('./country-series');

var now = Date.now();

for (var i = timeserie.length - 1; i >= 0; i--) {
    var series = timeserie[i];
    var decreaser = 0;
    for (var y = series.datapoints.length - 1; y >= 0; y--) {
        series.datapoints[y][1] = Math.round((now - decreaser) / 1000) * 1000;
        decreaser += 50000;
    }
}

var annotation = {
    name: "annotation name",
    enabled: true,
    datasource: "generic datasource",
    showLine: true,
}

var annotations = [
    {
        annotation: annotation,
        "title": "Donlad trump is kinda funny",
        "time": 1450754160000,
        text: "teeext",
        tags: "taaags"
    },
    {annotation: annotation, "title": "Wow he really won", "time": 1450754160000, text: "teeext", tags: "taaags"},
    {annotation: annotation, "title": "When is the next ", "time": 1450754160000, text: "teeext", tags: "taaags"}
];

var tagKeys = [
    {"type": "string", "text": "Country"}
];

var countryTagValues = [
    {'text': 'SE'},
    {'text': 'DE'},
    {'text': 'US'}
];

var now = Date.now();
var decreaser = 0;
for (var i = 0; i < annotations.length; i++) {
    var anon = annotations[i];

    anon.time = (now - decreaser);
    decreaser += 1000000
}

var table =
    {
        columns: [{text: 'Time', type: 'time'}, {text: 'Country', type: 'string'}, {text: 'Number', type: 'number'}],
        values: [
            [1234567, 'SE', 123],
            [1234567, 'DE', 231],
            [1234567, 'US', 321],
        ]
    };

function setCORSHeaders(res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "POST");
    res.setHeader("Access-Control-Allow-Headers", "accept, content-type");
}

function getTableFormat(columns, data) {
    let result = {type: 'table'};
    result.columns = [];
    result.rows = [];
    for (let i = 0; i < columns.length; i++) {
        let item = columns[i];
        result.columns.push({text: item.name, type: item.type});
    }

    for (let i = 0; i < data.length; i++) {
        let item = data[i];
        result.rows.push(item);
    }

    return result;
}

function getTimeFormat(columns, data) {
    let result = [];

    var pos_time_sec = -1;
    var pos_value = -1;
    var pos_metric = -1;

    for (let i = 0; i < columns.length; i++) {
        let item = columns[i];
        if (item.name == 'time_sec') {
            pos_time_sec = i;
        } else if (item.name == 'value') {
            pos_value = i;
        } else if (item.name == 'metric') {
            pos_metric = i;
        }
    }

    var tmp = {};
    for (let i = 0; i < data.length; i++) {
        let item = data[i];
        let item_time_sec = item[pos_time_sec];
        let item_value = item[pos_value];
        let item_metric = "";
        if (pos_metric != -1) {
            item_metric = item[pos_metric];
        } else {
            item_metric = "metric";
        }
        if (!tmp[item_metric]) {
            tmp[item_metric] = [];
        }
        tmp[item_metric].push([item_value, item_time_sec]);
    }

    for (let item in tmp) {
        result.push({target: item, datapoints: tmp[item]})
    }

    return result;
}

app.all('/', function (req, res) {
    setCORSHeaders(res);
    res.send('I have a quest for you!');
    res.end();
});

app.all('/search', function (req, res) {
    setCORSHeaders(res);
    var result = [];
    _.each(timeserie, function (ts) {
        result.push(ts.target);
    });

    res.json(result);
    res.end();
});

app.all('/annotations', function (req, res) {
    setCORSHeaders(res);
    console.log(req.url);
    console.log(req.body);

    res.json(annotations);
    res.end();
});

var presto = require('presto-client');
var JSONbig = require('json-bigint');
var client = new presto.Client({user: 'grafana', port: '8080', host: 'data-calc-01', jsonParser: JSONbig});
app.all('/query', function (req, res) {
    setCORSHeaders(res);
    console.log(req.url);
    console.log(req.body);

    var tsResult = [];
    let fakeData = timeserie;

    if (req.body.adhocFilters && req.body.adhocFilters.length > 0) {
        fakeData = countryTimeseries;
    }

    var range_model = "(${column_name} >= ${column_from_value} and ${column_name} <= ${column_to_value})";

    var reg = /\$__timeFilter\(@([0-9 _ a-z A-Z \( \) \'\"]*)@\)/mgi;
    var reg_inner = /\$__timeFilter\(@([0-9 _ a-z A-Z \( \) \'\"]*)@\)/;

    var each_count = 0;
    _.each(req.body.targets, function (target) {
        let sql = target.target;
        if (req.body.adhocFilters) {
            for (let i = 0; i < req.body.adhocFilters.length; i++) {
                let item = req.body.adhocFilters[i];
                sql = sql.replace("${" + item.key + "}", item.value);
            }
        }
        var m = sql.match(reg);
        if (m) {
            _.each(m, function (item) {
                var column_name = item.match(reg_inner)[1];
                var tmp_range = range_model.replace(/\$\{column_name\}/g, column_name)
                    .replace(/\$\{column_from_value\}/g, Math.round(new Date(req.body.range.from).getTime() / 1000))
                    .replace(/\$\{column_to_value\}/g, Math.round(new Date(req.body.range.to).getTime() / 1000));
                sql = sql.replace(item, tmp_range);
            })
        }

        let query_columns = [];
        let query_data = [];

        client.execute({
            query: sql,
            catalog: 'falcon',
            schema: 'default',
            source: 'nodejs-client',
            state: function (error, query_id, stats) {
                console.log(stats);
            },
            columns: function (error, data) {
                query_columns = data;
            },
            data: function (error, data, columns, stats) {
                query_data = data;
            },
            success: function (error, stats) {
                var format_data = {};
                if (target.type == "table") {
                    format_data = getTableFormat(query_columns, query_data);
                    format_data.target = target.target;
                    format_data.type = target.type;
                    tsResult.push(format_data);
                } else if (target.type == "timeserie") {
                    format_data = getTimeFormat(query_columns, query_data);
                    tsResult = format_data;
                }
                each_count++;
                if (each_count == req.body.targets.length) {
                    res.json(tsResult);
                    res.end();
                }
            },
            error: function (error) {
                each_count++;
                if (each_count == req.body.targets.length) {
                    res.json(tsResult);
                    res.end();
                }
            }
        });
    });
});

app.all('/tag[\-]keys', function (req, res) {
    setCORSHeaders(res);
    console.log(req.url);
    console.log(req.body);

    res.json(tagKeys);
    res.end();
});

app.all('/tag[\-]values', function (req, res) {
    setCORSHeaders(res);
    console.log(req.url);
    console.log(req.body);

    if (req.body.key == 'City') {
        res.json(cityTagValues);
    } else if (req.body.key == 'Country') {
        res.json(countryTagValues);
    }
    res.end();
});

app.listen(3333);

console.log("Server is listening to port 3333");
