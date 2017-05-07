from consecution import Pipeline as BasePipeline


class Pipeline(BasePipeline):

    def start(self):
        """
        Start the pipeline without passing an iterable. The top Node in the
        pipeline will be responsible for pushing the initial items.
        """
        self.begin()
        self.top_node._process(None)
        return self.end()
