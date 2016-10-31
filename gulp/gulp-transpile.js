'use strict'

const gulp = require('gulp'),
  changed = require('gulp-changed'),
  ts = require('gulp-typescript'),
  addsrc = require('gulp-add-src'),
  fs = require('fs'),
  util = require('util');

const sourceOursOnly = ['{src,test}/**/*.ts'];
const sourceOursJs = ['{src,test}/**/*.js'];
const sourceMainTS = 'typings/index.d.ts';
const sourcePath = ['{src,test}/**/*.ts', 'typings/index.d.ts']
const sourceDest = 'dist';

const tsProject = ts.createProject({
  emitDecoratorMetadata: true,
  experimentalDecorators: true,
  moduleResolution: "node",
  module: "commonjs",
  target: "ES6"
});

gulp.task('compile:main', function () {
  return gulp.src(sourceOursOnly)
    .pipe(changed(sourceDest, { extension: '.js' }))
    .pipe(addsrc(sourceMainTS))
    .pipe(tsProject())
    .pipe(addsrc(sourceOursJs))
    .on('error', () => process.exit(1))
    .pipe(gulp.dest(sourceDest));
});

gulp.task('default', ['compile:main']);
