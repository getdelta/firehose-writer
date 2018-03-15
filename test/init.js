const chai = require('chai')
global.expect = chai.expect

const sinon = require('sinon')
chai.use(require('sinon-chai'))

chai.use(require("chai-as-promised"))

beforeEach(function() {
  this.sinon = sinon.sandbox.create()
})

afterEach(function() {
  this.sinon.restore()
})
