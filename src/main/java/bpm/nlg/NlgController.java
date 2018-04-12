package bpm.nlg;

import simplenlg.framework.*;
import simplenlg.lexicon.*;
import simplenlg.lexicon.spanish.XMLLexicon;
import simplenlg.realiser.spanish.*;

public class NlgController {

    Lexicon lexicon = new XMLLexicon();
    NLGFactory nlgFactory = new NLGFactory(lexicon);
    Realiser realiser = new Realiser(lexicon);

    public void test(){
        NLGElement s1 = nlgFactory.createSentence("mi perro es feliz");
        String output = realiser.realiseSentence(s1);
        System.out.println(output);
    }

}
