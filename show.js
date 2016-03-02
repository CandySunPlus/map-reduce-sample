'use strict';
import {MongoClient} from 'mongodb'
import assert from 'assert'
import fs from 'fs'

const url = 'mongodb://localhost:27017/data_analyst_db';

const beginDate = new Date('Tue Feb 25 2016 00:00:00 GMT+0800 (CST)');
const endDate = new Date('Tue Feb 29 2016 00:00:00 GMT+0800 (CST)');

function tmp_map() {
  let year = this.date.getFullYear();
  let month = this.date.getMonth() + 1;
  let day = this.date.getDate();
  let hour = this.date.getHours();
  let minutes = this.date.getMinutes();
  // let date_string = `${year}-${month}-${day} ${hour}:${minutes}`;
  let date_string = `${year}-${month}-${day}`;
  let cate = null;
  let area = null;
  let name = this.name;
  const areas = ['hd', 'hn', 'hb'];
  if (this.name.includes('频道首页')) {
    cate = '频道首页';
  } else if (this.name.includes('频道')) {
    if (this.name.includes('hd-fruit')) {
      name = '频道页:hb-fruit';
    } else if (this.name.includes('hb-fruit')) {
      name = '频道页:hd-fruit';
    }
    for (let _area of areas) {
      if (name.includes(_area)) {
        cate = name.replace(`${_area}-`, '').replace(_area, '').trim();
        area = _area;
        break;
      }
    }
  }
  emit({'date':date_string, user_id: this.user_id, 'area': area, 'cate': cate, 'source': this.source, 'name': name}, 1);
};

function tmp_reduce(key, values) {
  return Array.sum(values)
}

function map() {
  emit({'date':this._id.date, 'user_id': this._id.user_id, 'col': this._id.name, 'source': this._id.source}, this.value);
};

function area_map() {
  emit({'date':this._id.date, 'user_id': this._id.user_id, 'col': this._id.area, 'source': this._id.source}, this.value);
};

function cate_map() {
  emit({'date':this._id.date, 'user_id': this._id.user_id, 'col': this._id.cate, 'source': this._id.source}, this.value);
};

function result_map() {
  emit({'date': this._id.date, 'col': this._id.col, 'source': this._id.source}, {pv: this.value, uv: 1});
}

function result_reduce(key, values) {
  let _pvi = [];
  let _uvi = [];
  for (let item of values) {
    _pvi.push(item.pv);
    _uvi.push(item.uv);
  }
  return {pv: Array.sum(_pvi), uv: Array.sum(_uvi)};
};

function output_result(name, collection, col) {
  let columns = ['date', col, 'source', 'pv', 'uv'];
  let cursor = collection.find();
  fs.appendFile(name, columns.join(',') + '\r\n', {flag: 'a+'});
  cursor.each((err, doc) => {
    ;
    if (doc != null) {
      let date = doc._id.date;
      let cus = doc._id.col;
      if (name.includes('full_uv_pv') && !cus.includes('频道')) {
        return;
      }
      if (cus == null) {
        return;
      }
      let source = doc._id.source;
      let pv = doc.value.pv;
      let uv = doc.value.uv;
      fs.appendFile(`./result/${name}`, [date, cus, source, pv, uv].join(',') + '\r\n', {flag: 'a+'});
    } else {
      console.log(`${name} output done.`);
    }
  });
}

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log('connect correctly to server');
  let collection = db.collection('access_log_collection');
  collection.mapReduce(tmp_map, tmp_reduce, {
    query: {
      date: {
        $gte: beginDate,
        $lt: endDate
      }
    },
    out: 'my_tmp_result'
  }, (err, results) => {
    if (err) {
      console.log(err);
    } else {
      console.log('tmp map-reduce done.');
      let collection = db.collection('my_tmp_result');
      collection.mapReduce(map, tmp_reduce, {
        out: 'full_tmp_result'
      }, (err, results) => {
        if (err) {
          console.log(err);
        } else {
          console.log('full tmp map-reduce done.');
          let full_tmp_result = db.collection('full_tmp_result');
          full_tmp_result.mapReduce(result_map, result_reduce, {
            out: 'full_result'
          }, (err, results) => {
            if (err) {
              console.log(err);
            } else {
              output_result('full_uv_pv.csv', db.collection('full_result'), 'name');
              console.log('full map-reduce done.');
              full_tmp_result.drop();
            }
          });
        }
      });
      collection.mapReduce(area_map, tmp_reduce, {
        out: 'area_tmp_result'
      }, (err, results) => {
        if (err) {
          console.log(err);
        } else {
          console.log('area tmp map-reduce done.');
          let area_tmp_result = db.collection('area_tmp_result');
          area_tmp_result.mapReduce(result_map, result_reduce, {
            out: 'area_result'
          }, (err, results) => {
            if (err) {
              console.log(err);
            } else {
              output_result('area_uv_pv.csv', db.collection('area_result'), 'area');
              console.log('area map-reduce done.');
              area_tmp_result.drop();
            }
          });
        }
      });
      collection.mapReduce(cate_map, tmp_reduce, {
        out: 'cate_tmp_result'
      }, (err, results) => {
        if (err) {
          console.log(err);
        } else {
          console.log('cate tmp map-reduce done.');
          let cate_tmp_result = db.collection('cate_tmp_result');
          cate_tmp_result.mapReduce(result_map, result_reduce, {
            out: 'cate_result'
          }, (err, results) => {
            if (err) {
              console.log(err);
            } else {
              output_result('cate_uv_pv.csv', db.collection('cate_result'), 'cate');
              console.log('cate map-reduce done.');
              cate_tmp_result.drop();
            }
          });
        }
      });
    }
  });
// let promise = collection.count({
//   name: '频道页:hb-fruit',
//   date: {
//     $gte: beginDate,
//     $lt: endDate
//   }
// });
// promise.then(res => {
//   console.log(res);
// });
});
