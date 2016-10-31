// Global base path for gulp scripts
global.gulpBase = __dirname;
global.pluginBridge = { require: require };

require('./gulp/gulp-clean');
require('./gulp/gulp-transpile');
