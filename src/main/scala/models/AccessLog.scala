package models

case class AccessLog(
  ip: String,
  ident: String,
  user: String,
  datetime: String,
  request: String,
  status: String,
  size: String,
  referer: String,
  userAgent: String,
  unk: String)
