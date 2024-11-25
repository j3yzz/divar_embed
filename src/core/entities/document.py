class Document:
    def __init__(
            self,
            doc_id,
            ad_description,
            title,
            categories,
            location,
            price,
            embedding_vector=None
    ):
        self.doc_id = doc_id
        self.ad_description = ad_description
        self.title = title
        self.categories = categories or {}
        self.location = location
        self.embedding_vector = embedding_vector
        self.price = price

    def to_dict(self):
        return {
            "_id": self.doc_id,
            "ad_description": self.ad_description,
            "embedding_vector": self.embedding_vector,
            "title": self.title,
            "categories": self.categories,
            "location": self.location,
            "price": self.price,
        }
