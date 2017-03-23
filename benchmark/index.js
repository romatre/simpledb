var express = require('express');
var app = express();

app.use(express.static('public'));

app.get('/data', function(req, res){
    fs = require('fs')
    fs.readFile('./data.json', 'utf8', function (err, resultFile) {
        if (err) {
            res.send(err);
        }
        const result = [];
        const baseTrace = { x: [], y: [], name: '', type: 'bar' };

        let data = JSON.parse(resultFile);

        data[0].data.forEach(stat => {
            var traceW = Object.assign({}, baseTrace, {
                x: data.map(d => d.statName),
                y: data.reduce((prev, current) => {
                    current.data.forEach(s => prev.push(s));
                    return prev;
                }, [])
                    .filter(d => d.type === stat.type)
                    .map(d => d.data.filter(t => t.fileName === 'students.tbl')[0].writtenBlocks),
                name: stat.type + '(writtenBlocks)',
            });
            var traceR = Object.assign({}, baseTrace, {
                x: data.map(d => d.statName),
                y: data.reduce((prev, current) => {
                    current.data.forEach(s => prev.push(s));
                    return prev;
                }, [])
                    .filter(d => d.type === stat.type)
                    .map(d => d.data.filter(t => t.fileName === 'students.tbl')[0].readBlocks),
                name: stat.type + '(readBlocks)',
            });
            result.push(traceW);
            result.push(traceR);
        });
        res.send(result);
    });
});

app.listen(3000, () => {
    console.log("Server on http://localhost:3000");
});