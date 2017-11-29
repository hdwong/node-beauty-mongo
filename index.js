"use strict";
let core, config, logger, db, m = require('mongodb'), _ = require('lodash');

let serviceName = 'solr';
let mongo = {
  assert: (error) => {
    if (error) {
      logger.error(error);
      throw '[' + serviceName + '] ' + error;
    }
  },
  init: (name, c, next) => {
    serviceName = name;
    core = c;
    logger = core.getLogger(serviceName);
    config = core.getConfig(serviceName);
    let client = m.MongoClient, url = 'mongodb://' +
        (config.host || '127.0.0.1') + ':' + (config.port || 27017) +
        '/' + (config.name || '');
    client.connect(url, { db: { native_parser: true }}, (error, instance) => {
      mongo.assert(error);
      db = instance;
      db.on('error', mongo.assert);
      if (config.user === undefined || config.password === undefined) {
        next();
      } else {
        db.authenticate(config.user || 'sa', config.password || '', (error, result) => {
          mongo.assert(error);
          next();
        });
      }
    });
  },
  uninit: (core) => {
    if (db) {
      db.close();
    }
  },
  getCollection: (collection) => {
    return db.collection(collection);
  },
  getObjectId: (id) => {
    return new m.ObjectID(id);
  },
  post_insert: (req, res, next) => {
    if (req.body.collection === undefined || req.body.set === undefined) {
      throw 'Params is wrong';
    }
    let set, options;
    try {
      set = JSON.parse(req.body.set);
      set = _.isArray(set) ? set : [set];
      if (req.body.options) {
        options = JSON.parse(req.body.options);
      }
    } catch (e) {
      throw 'Params is wrong';
    }
    db.collection(req.body.collection).insertMany(set, options, (error, result) => {
      mongo.assert(error);
      next(result);
    });
  },
  post_find: (req, res, next) => {
    if (req.body.collection === undefined || req.body.query === undefined) {
      throw 'Params is wrong';
    }
    let query, options;
    try {
      query = JSON.parse(req.body.query);
      if (req.body.options) {
        options = JSON.parse(req.body.options);
      }
    } catch (e) {
      throw 'Params is wrong';
    }
    if (typeof options !== 'object') {
      options = {};
    }
    db.collection(req.body.collection).find(query, options).toArray((error, result) => {
      mongo.assert(error);
      next(result);
    });
  },
  post_count: (req, res, next) => {
    if (req.body.collection === undefined || req.body.query === undefined) {
      throw 'Params is wrong';
    }
    let query, options;
    try {
      query = JSON.parse(req.body.query);
      if (req.body.options) {
        options = JSON.parse(req.body.options);
      }
    } catch (e) {
      throw 'Params is wrong';
    }
    if (typeof options !== 'object') {
      options = {};
    }
    db.collection(req.body.collection).count(query, options, (error, result) => {
      mongo.assert(error);
      next(result);
    });
  },
  post_updated: function(req, res, next) {
    if (req.body.collection === undefined ||
        req.body.where === undefined ||
        req.body.set === undefined) {
      throw '参数错误';
    }
    var set, options, where;
    try {
      set = JSON.parse(req.body.set);
      set = _.isArray(set) ? set : [set];
      where = JSON.parse(req.body.where);
      where = _.isArray(where) ? set : [where];
      if (req.body.options) {
        options = JSON.parse(req.body.options);
      }
    } catch (e) {
      throw '参数错误';
    }
    db.collection(req.body.collection).insertMany(set, options, function(error, result) {
      mongo.assert(error);
      next(result);
    });
  }
};

module.exports = mongo;
