class Document:
    def __init__(self, doc_id, ad_description, embedding_vector=None):
        self.doc_id = doc_id
        self.ad_description = ad_description
        self.embedding_vector = embedding_vector

    def to_dict(self):
        return {
            "_id": self.doc_id,
            "ad_description": self.ad_description,
            "embedding_vector": self.embedding_vector,
        }
