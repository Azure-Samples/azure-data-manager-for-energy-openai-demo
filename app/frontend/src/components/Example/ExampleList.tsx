import { Example } from "./Example";

import styles from "./Example.module.css";

export type ExampleModel = {
    text: string;
    value: string;
};

const EXAMPLES: ExampleModel[] = [
    {
        text: "Tell me about wellbore 1007?",
        value: "Tell me about wellbore 1007?"
    },
    { text: "What are the coordinates of wellbore 1014?", value: "What are the coordinates of wellbore 1014?" },
    { text: "What is the spud date of wellbore 1014?", value: "What is the spud date of wellbore 1014?" }
];

interface Props {
    onExampleClicked: (value: string) => void;
}

export const ExampleList = ({ onExampleClicked }: Props) => {
    return (
        <ul className={styles.examplesNavList}>
            {EXAMPLES.map((x, i) => (
                <li key={i}>
                    <Example text={x.text} value={x.value} onClick={onExampleClicked} />
                </li>
            ))}
        </ul>
    );
};
