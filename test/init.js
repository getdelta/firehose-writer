const chai = require('chai')
global.expect = chai.expect

const sinon = require('sinon')
chai.use(require('sinon-chai'))

beforeEach(function() {
  this.sinon = sinon.sandbox.create()
})

afterEach(function() {
  this.sinon.restore()
})
