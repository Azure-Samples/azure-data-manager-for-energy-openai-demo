import openai
from approaches.approach import Approach
from azure.search.documents import SearchClient
from azure.search.documents.models import QueryType
from text import nonewlines

# Simple retrieve-then-read implementation, using the Cognitive Search and OpenAI APIs directly. It first retrieves
# top documents from search, then constructs a prompt with them, and then uses OpenAI to generate an completion 
# (answer) with that prompt.
class RetrieveThenReadApproach(Approach):

    template = \
"You are an intelligent assistant helping Contoso Inc employees with their data stored in Azure Data Manager for Energy (ADME). " + \
"Use 'you' to refer to the individual asking the questions even if they ask with 'I'. " + \
"For tabular information return it as an html table. Do not return markdown format. "  + \
"""
"""

    def __init__(self, search_client: SearchClient, openai_deployment: str, content_field: str):
        self.search_client = search_client
        self.openai_deployment = openai_deployment
        self.content_field = content_field

    def run(self, q: str, overrides: dict) -> any:
        use_semantic_captions = True if overrides.get("semantic_captions") else False
        top = overrides.get("top") or 3
        exclude_category = overrides.get("exclude_category") or None
        filter = "category ne '{}'".format(exclude_category.replace("'", "''")) if exclude_category else None

        
        results = self.search_client.search(search_text=q, top=3)
        fullreturn = ""
        for result in results:
            if result['@search.score'] > 0.8:
                print("High confidence: " + str(result['@search.score']))
                for x in result:                
                    if result[x] is not None and result[x] != "" and result[x] != "None" and "@" not in str(x):
                        fullreturn += x + ": " + str(result[x]) + "\n"     
        print('fullreturn: ' +fullreturn)
        #print('content: '+content)

        prompt = (overrides.get("prompt_template") or self.template).format(q=q, retrieved=fullreturn)
        completion = openai.Completion.create(
            engine=self.openai_deployment, 
            prompt=prompt, 
            temperature=overrides.get("temperature") or 0.3, 
            max_tokens=1024, 
            n=1, 
            stop=["\n"])
        #results = []
        #results.append("Wellbore 1019 is in Norway")
        #result.append("Wellbore 1019 is abandoned")
        theEnd = {"data_points": fullreturn, "answer": completion.choices[0].text, "thoughts": f"Question:<br>{q}<br><br>Prompt:<br>" + prompt.replace('\n', '<br>')}
        print(theEnd)
        return theEnd
        #return {"data_points": results, "answer": "I am retrievethenread", "thoughts": f"Question:<br>{q}<br><br>Prompt:<br>" + prompt.replace('\n', '<br>')}
