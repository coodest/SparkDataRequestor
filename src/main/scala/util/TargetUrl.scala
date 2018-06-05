package util

class TargetUrl(var target: String) {
  private var kind: String = null
  private var requestTime: Int = 5

  def getTarget: String = {
    target
  }

  def setTarget(target: String) {
    this.target = target
  }

  def getType: String = {
    kind
  }

  def setType(kind: String) {
    this.kind = kind
  }

  def getRequestTime: Int = {
    requestTime
  }

  def setRequestTime(requestTime: Int) {
    this.requestTime = requestTime
  }

  def decreaseRequestTime {
    requestTime = requestTime - 1
  }
}

