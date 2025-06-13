"""
A Sample Web-DB Application for DB-DESIGN lecture
Copyright (C) 2022 Yasuhiro Hayashi
"""
from flaskdb import db

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password = db.Column(db.String(128), nullable=False)
    # items = db.relationship("Item", backref="Items.owner_id")

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != '_sa_instance_state' }

    def from_dict(self, d):
        { setattr(self, k, v) for k, v in d.items() }

    def __repr__(self):
        return "<User %r>" % self.id

class Item(db.Model):
    __tablename__ = "items"
    id = db.Column(db.Integer, primary_key=True)
    owner_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    itemname = db.Column(db.String(128), nullable=False)
    price = db.Column(db.Integer(), nullable=False)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != '_sa_instance_state' }

    def from_dict(self, d):
        { setattr(self, k, v) for k, v in d.items() }

    def __repr__(self):
        return "<Item %r>" % self.id

class Order(db.Model):
    __tablename__ = "orders"
    id = db.Column(db.Integer, primary_key=True)
    order_code = db.Column(db.String, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    item_id = db.Column(db.Integer, db.ForeignKey("items.id"), nullable=False)
    price = db.Column(db.Integer, nullable=False)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != '_sa_instance_state' }

    def from_dict(self, d):
        { setattr(self, k, v) for k, v in d.items() }

    def __repr__(self):
        return "<Order %r>" % self.id
